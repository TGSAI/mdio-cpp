// Copyright 2024 TGS
 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
 
//    http://www.apache.org/licenses/LICENSE-2.0
 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <nlohmann/json.hpp>
#include <optional>

#include "tensorstore/tensorstore.h"

/**
 * The intention of this extensible data class is to provide a representation of the UserAttributes that COULD be
 * modified. It is intended to be completely immutable after construction. The reasoning is that this data is already
 * written to durable media and if the wrong values are changed the data could be corrupted. This is a safety measure to
 * prevent that. Note that modifications are not durable and will need to be committed from the parent Dataset object.
 * Great care must be taken when committing the changes and should only be done once.
 * While there are multiple construction options, the only one intended for user interaction is
 * `mdio::UserAttributes::FromJson(const nlohmann::json)`. The UserAttributes that is associated with the Variable
 * should be reassigned to a new UserAttributes object iff the object NEEDS to be modified. A normal usecase should NOT
 * require constant modification of the UserAttributes object.
 *
 * The end-user should NEVER need to enter the internal namespace or access the UserAttributes constructor directly.
 * They should instead use the static member function FromJson.
 * If a UserAttributes object is in need of modification it should be done as follows
 *
 * @code
 * // NOTE: We do not verify the status of `dataset.variables.at("variable")` in this example code. This is for brevity.
 * mdio::UserAttributes userAttrs = dataset.variables.at("variable").value().userAttrs;
 * nlohmann::json updatedAttrs = userAttrs.ToJson();
 * // Modify updatedAttrs as any normal JSON object
 * // NOTE: FromJson can take an optional template of `int32_t` default to `float` to specify the type of the histogram.
 * auto updatedUserAttrsResult = mdio::UserAttributes::FromJson(updatedAttrs);
 * if (!updatedUserAttrsResult.ok()) {
 *   // Handle error
 * }
 * mdio::UserAttributes updatedUserAttrs = updatedUserAttrsResult.value();
 * dataset.variables.get("variable").value().userAttrs = updatedUserAttrs;
 * @endcode
 *
 * Another thing to note is that we do not supply an easy way to add a histogram or attributes to an existing
 * UserAttributes object. This is by design, a defined Variable should know beforehand that it will contain those
 * attributes.
 */

namespace mdio {
namespace internal {

/**
 * @brief A Histogram can be either CenteredBinHistogram or EdgeDefinedHistogram as defined by the MDIO spec
 */
class Histogram {
  public:
    virtual ~Histogram() = default;
    virtual nlohmann::json getHistogram() const = 0;
    virtual std::unique_ptr<const Histogram> clone() const = 0;
    virtual tensorstore::Result<std::unique_ptr<const Histogram>> FromJson(const nlohmann::json& j) const = 0;
    /**
     * @brief Notifier for whether or not the histogram is bindable
     * There is an instance where the histogram is not bindable, such as when we need a placeholder internally.
     * @return True if the histogram is MDIO bindable, flase otherwise.
     */
    virtual bool isBindable() const = 0;

    const std::string HIST_KEY = "histogram";

  private:
    /**
     * @brief Implementation specific method to determine if the JSON object is a histogram
     * @param j The JSON object that may be a histogram
     * @return True if the JSON is a histogram, false otherwise
     */
    virtual bool isHist(const nlohmann::json& j) const = 0;
};

template <typename T = float>
class CenteredBinHistogram : public Histogram {
  public:
    CenteredBinHistogram(const std::vector<T>& binCenters, const std::vector<int32_t>& counts)
        : binCenters(binCenters), counts(counts) {
        static_assert(std::is_same_v<T, float> || std::is_same_v<T, int32_t>,
                      "Histograms may only be float32 or int32_t.");
    }

    tensorstore::Result<std::unique_ptr<const Histogram>> FromJson(const nlohmann::json& j) const override {
        if (isHist(j)) {
            auto histogram = j[HIST_KEY];
            if (histogram.contains("binCenters") && histogram.contains("counts")) {
                std::vector<T> binCenters = j[HIST_KEY]["binCenters"];
                std::vector<int32_t> counts = j[HIST_KEY]["counts"];
                auto hist = std::make_unique<CenteredBinHistogram<T>>(binCenters, counts);
                return tensorstore::Result<std::unique_ptr<const Histogram>>(std::move(hist));
            }
            return absl::InvalidArgumentError("Error parsing histogram:\n\tType detected: "
                                              "CenteredBinHistogram\n\tMissing child key: 'binCenters' or 'counts'");
        }
        return absl::InvalidArgumentError(
            "Error parsing histogram:\n\tType detected: CenteredBinHistogram\n\tMissing parent key: '" + HIST_KEY +
            ";");
    }

    nlohmann::json getHistogram() const override {
        nlohmann::json histogram;
        histogram[HIST_KEY]["binCenters"] = this->binCenters;
        histogram[HIST_KEY]["counts"] = this->counts;
        return histogram;
    }

    std::unique_ptr<const Histogram> clone() const override {
        return std::make_unique<CenteredBinHistogram>(*this);
    }

    bool isBindable() const override {
        return true;
    }

  private:
    const std::vector<T> binCenters;
    const std::vector<int32_t> counts;

    bool isHist(const nlohmann::json& j) const override {
        return j.contains(HIST_KEY);
    }
};

template <typename T = float>
class EdgeDefinedHistogram : public Histogram {
  public:
    EdgeDefinedHistogram(const std::vector<T>& binEdges,
                         const std::vector<T>& binWidths,
                         const std::vector<int32_t>& counts)
        : binEdges(binEdges), binWidths(binWidths), counts(counts) {
        static_assert(std::is_same_v<T, float> || std::is_same_v<T, int32_t>,
                      "Histograms may only be float32 or int32_t.");
    }

    /**
     * @brief Attempts to construct a new EdgeDefinedHistogram from a JSON representation
     * @param j The JSON representation of the histogram
     * @return A new EdgeDefinedHistogram if the input JSON is valid, otherwise an error
     */
    tensorstore::Result<std::unique_ptr<const Histogram>> FromJson(const nlohmann::json& j) const override {
        if (isHist(j)) {
            auto histogram = j[HIST_KEY];
            if (histogram.contains("binEdges") && histogram.contains("binWidths") && histogram.contains("counts")) {
                std::vector<T> binEdges = j[HIST_KEY]["binEdges"];
                std::vector<T> binWidths = j[HIST_KEY]["binWidths"];
                std::vector<int32_t> counts = j[HIST_KEY]["counts"];
                auto hist = std::make_unique<EdgeDefinedHistogram<T>>(binEdges, binWidths, counts);
                return tensorstore::Result<std::unique_ptr<const Histogram>>(std::move(hist));
            }
            // TODO: Provide better descriptive error message here.
            return absl::InvalidArgumentError(
                "Error parsing histogram:\n\tType detected: EdgeDefinedHistogram\n\tMissing child key: 'binEdges', "
                "'binWidths', or 'counts'");
        }
        return absl::InvalidArgumentError(
            "Error parsing histogram:\n\tType detected: EdgeDefinedHistogram\n\tMissing parent key: 'histogram'");
    }

    nlohmann::json getHistogram() const override {
        nlohmann::json histogram;
        histogram[HIST_KEY]["binEdges"] = this->binEdges;
        histogram[HIST_KEY]["binWidths"] = this->binWidths;
        histogram[HIST_KEY]["counts"] = this->counts;
        return histogram;
    }

    std::unique_ptr<const Histogram> clone() const override {
        return std::make_unique<EdgeDefinedHistogram>(*this);
    }

    bool isBindable() const override {
        return true;
    }

  private:
    const std::vector<T> binEdges;
    const std::vector<T> binWidths;
    const std::vector<int32_t> counts;

    bool isHist(const nlohmann::json& j) const override {
        return j.contains(HIST_KEY);
    }
};

class SummaryStats {

  public:
    SummaryStats(const SummaryStats& other)
        : count(other.count), max(other.max), min(other.min), sum(other.sum), sumSquares(other.sumSquares),
          histogram(other.histogram->clone()) {
    }

    const nlohmann::json getBindable() const {
        nlohmann::json stats = this->histogram->getHistogram();
        stats["count"] = this->count;
        stats["max"] = this->max;
        stats["min"] = this->min;
        stats["sum"] = this->sum;
        stats["sumSquares"] = this->sumSquares;
        return stats;
    }

    /**
     * @brief Constructs a statsV1 object from JSON
     * It is assumed to be a singleton of a statsV1 object.
     * This should only be used internally
     * @tparam T Type of the summary histogram
     * @param j The JSON of a statsV1 object
     * @return A result of the constructed summary stats
     */
    template <typename T = float>
    static tensorstore::Result<SummaryStats> FromJson(const nlohmann::json j) {
        auto histRes = constructHist<T>(j);
        if (!histRes.status().ok()) {
            return histRes.status();
        }
        auto histogram = std::move(histRes.value());
        auto stats = SummaryStats(j["count"].get<int32_t>(),
                                  j["max"].get<float>(),
                                  j["min"].get<float>(),
                                  j["sum"].get<float>(),
                                  j["sumSquares"].get<float>(),
                                  std::move(histogram));
        return tensorstore::Result<SummaryStats>(stats);
    }

  private:
    SummaryStats(const int32_t count,
                 const float max,
                 const float min,
                 const float sum,
                 const float sumSquares,
                 std::unique_ptr<const Histogram> histogram)
        : count(count), max(max), min(min), sum(sum), sumSquares(sumSquares), histogram(std::move(histogram)) {
    }
    /**
     * @brief A type agnostic Histogram factory method
     * @tparam T Type of the histogram (Default: float)
     * @param stats The statsV1 JSON. Expects a root of "statsV1"
     * @return A unique pointer to a Histogram object or error result if the JSON is invalid
     */
    template <typename T = float>
    static tensorstore::Result<std::unique_ptr<const Histogram>> constructHist(const nlohmann::json& stats) {
        if (!stats.contains("histogram")) {
            return absl::InvalidArgumentError("Error parsing histogram:\n\tMissing parent key: 'histogram'");
        }
        std::unique_ptr<const Histogram> histogram;
        if (stats["histogram"].contains("binCenters") && stats["histogram"].contains("counts")) {
            auto inertHist = CenteredBinHistogram<T>({}, {});
            auto res = inertHist.FromJson(stats);
            if (!res.status().ok()) {
                return res.status();
            }
            histogram = std::move(res.value());
        } else if (stats["histogram"].contains("binEdges") && stats["histogram"].contains("binWidths") &&
                   stats["histogram"].contains("counts")) {
            auto inertHist = EdgeDefinedHistogram<T>({}, {}, {});
            auto res = inertHist.FromJson(stats);
            if (!res.status().ok()) {
                return res.status();
            }
            histogram = std::move(res.value());
        } else {
            // This should never be true
            return absl::InvalidArgumentError("Could not deduce the type of the provided histogram.");
        }
        return histogram;
    }

    const int32_t count;
    const float max;
    const float min;
    const float sum;
    const float sumSquares;
    const std::unique_ptr<const Histogram> histogram;
};

} // namespace internal

class UserAttributes {

  public:
      /**
     * @brief Copy constructor
     * @param other The UserAttributes object to copy
     * @note This constructor is intended for internal use only. Please use the static member function
     * `FromJson(nlohmann::json)`
     * @code
     * auto attrs = UserAttributes::FromJson(j).value(); // j is some valid nlohmann::json object
     * nlohmann::json newAttrs = attrs.ToJson();
     * newAttrs["attributes"]["newKey"] = "newValue";
     * auto newAttrsRes = UserAttributes::FromJson(newAttrs).value();
     * @endcode
     */
    UserAttributes(const UserAttributes& other) : stats(other.stats), attrs(other.attrs) {
    }

    /**
     * @brief Constructs a UserAttributes object from a JSON representation of a Dataset.
     * This is intended to function as an automated factory method for Dataset construction.
     * It should NEVER be invoked manually. Instead use the static member function `FromJson(nlohmann::json)`.
     * @tparam T The type of the histogram (May either be flaot or int32_t) (Default: float)
     * @param j The JSON representation of the dataset
     * @param varible The name of the Variable which may contain the user attributes
     * @return A UserAttributes object if the variable is a Variable in the Dataset, otherwise an error
     * @pre The input JSON must be validated according to the MDIO schema. This is unchecked and may have undefined
     * behavior if not followed.
     */
    static tensorstore::Result<UserAttributes> FromDatasetJson(const nlohmann::json& dataset,
                                                               const std::string& variable) {
        for (auto& var : dataset["variables"]) {
            if (var["name"] == variable) {
                auto param = var.contains("metadata") ? var["metadata"] : nlohmann::json::object();
                if (inferIsFloat(param)) {
                    return FromJson<float>(param);
                } else {
                    return FromJson<int32_t>(param);
                }
            }
        }
        return absl::InvalidArgumentError("Variable " + variable + " not found in Dataset");
    }

    static tensorstore::Result<UserAttributes> FromVariableJson(const nlohmann::json& variable) {
        auto param = variable.contains("metadata") ? variable["metadata"] : nlohmann::json::object();
        if (inferIsFloat(param)) {
            return FromJson<float>(param);
        } else {
            return FromJson<int32_t>(param);
        }
    }

    /**
     * @brief Constructs a UserAttributes object from the JSON representation of a UserAttributes
     * @tparam T The type of the histogram (May either be flaot or int32_t) (Default: float)
     * @param j The JSON representation of the UserAttributes
     * @return A UserAttributes object matching the input JSON
     */
    template <typename T = float>
    static tensorstore::Result<UserAttributes> FromJson(const nlohmann::json& j) {
        // Because the user can supply JSON here, there's a chance that the JSON is malformed.
        try {
            if (j.contains("statsV1")) {
                auto statsJson = j["statsV1"];
                std::vector<internal::SummaryStats> statsCollection;
                if (statsJson.is_array()) {
                    for (auto& s : statsJson) {
                        auto statsRes = internal::SummaryStats::FromJson<T>(s);
                        if (!statsRes.status().ok()) {
                            return statsRes.status();
                        }
                        statsCollection.emplace_back(statsRes.value());
                    }
                } else {
                    auto statsRes = internal::SummaryStats::FromJson<T>(statsJson);
                    if (!statsRes.status().ok()) {
                        return statsRes.status();
                    }
                    statsCollection.emplace_back(statsRes.value());
                }
                auto attrs = UserAttributes(statsCollection,
                                            j.contains("attributes") ? j["attributes"] : nlohmann::json::object());
                return attrs;
            } else if (j.contains("attributes")) {
                auto attrs = UserAttributes(j["attributes"]);
                return attrs;
            }
            auto attrs = UserAttributes(nlohmann::json::object());
            return attrs;
        } catch (const nlohmann::json::exception& e) {
            return absl::InvalidArgumentError("There appeared to be some malformed JSON" + std::string(e.what()));
        } catch (const std::exception& e) {
            return absl::InternalError("An unexpected error occurred: " + std::string(e.what()));
        }
    }

    /**
     * @brief Extracts just the statsV1 JSON
     * @return The statsV1 JSON representation of the data
     */
    const nlohmann::json getStatsV1() const {
        return statsBindable();
    }

    /**
     * @brief Extracts just the attributes JSON
     * @return The attributes JSON representation of the data
     */
    const nlohmann::json getAttrs() const {
        return attrsBindable();
    }

    /**
     * @brief Gets the JSON representation of a UserAttributes object
     * @return An nlohamnn json object
     */
    const nlohmann::json ToJson() const {
        nlohmann::json j = nlohmann::json::object();
        if (stats.size() >= 1) {
            j["statsV1"] = statsBindable();
        }
        auto attrs = attrsBindable();
        if (attrs.empty()) {
            return j;
        }
        j["attributes"] = attrs;
        return j;
    }

  private:
    /**
     * @brief A case where there are attributes but no statsV1 object.
     * @param attrs User specified attributes
     * @note This constructor is intended for internal use only. Please use the static member function
     * `FromJson(nlohmann::json)`
     */
    UserAttributes(const nlohmann::json& attrs) : attrs(attrs), stats({}) {
    }

    /**
     * @brief A case where there are statsV1 objects but no attributes
     * @param stats A collection of SummaryStats objects
     * @param attrs User specified attributes
     * @note This constructor is intended for internal use only. Please use the static member function
     * `FromJson(nlohmann::json)`
     */

    UserAttributes(const std::vector<internal::SummaryStats>& stats, const nlohmann::json attrs)
        : stats(stats), attrs(attrs) {
    }

    /**
     * @brief Binds the existing statsV1 data to a JSON object
     * @return A bindable statsV1 JSON object
     */
    const nlohmann::json statsBindable() const {
        if (stats.size() == 0) {
            return nlohmann::json::object();
        } else if (stats.size() == 1) {
            return stats[0].getBindable();
        }
        nlohmann::json statsRet = nlohmann::json::array();
        for (auto& stat : stats) {
            statsRet.emplace_back(stat.getBindable());
        }
        return statsRet;
    }

    /**
     * @brief Binds the existing attributes data to a JSON object
     * @return A bindable attributes JSON object
     */
    const nlohmann::json attrsBindable() const {
        if (!(attrs.empty())) {
            return attrs;
        }
        return nlohmann::json::object();
    }

    /**
     * @brief Attempts to infer if the JSON object should be a float or int32_t
     * Assumption: A Histogram will be float and can only be an integer if no decimal values are detected anywhere
     * inside of it.
     * Assumption: If there is a statsV1 array then the histogram is a float.
     * @param j The JSON object to infer the type of
     * @return True if the JSON object is infered to be a float
     */
    static bool inferIsFloat(const nlohmann::json& j) {
        if (j.contains("statsV1")) {
            if (j["statsV1"].is_array()) {
                return true; // Assumption #2
            }
            if (j["statsV1"].contains("histogram")) {
                for (const auto& val : j["statsV1"]["histogram"]) {
                    if (val.is_number_float() || (val.is_number_integer() && val != static_cast<int>(val))) {
                        return true; // Assumption #1
                    }
                }
                return false; // If we get here then we have only integers
            }
        }
        return true; // We don't care, a histogram doesn't exist in this Variable
    }

    std::vector<internal::SummaryStats> stats;
    const nlohmann::json attrs;
};

} // namespace mdio