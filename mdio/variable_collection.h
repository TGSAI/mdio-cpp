#include "variable.h"

namespace mdio {
/**
 * @brief A collection of variables.
 * Provides type erasure for the coordinates and variables.
 * This is intended to be an underlying data structure for the Dataset class.
 */
class VariableCollection {
 public:
  // Default constructor
  VariableCollection() = default;

  VariableCollection(
      std::initializer_list<std::pair<const std::string, Variable<>>> list)
      : variables(list) {}

  /**
   * @brief Adds a variable with the specified label to the dataset.
   *
   * If a variable with the same label already exists, it will be overwritten.
   *
   * @param label The label of the variable.
   * @param variable The variable to be added.
   */
  void add(const std::string& label, const Variable<>& variable) {
    variables[label] = variable;
  }

  /**
   * Retrieves a variable from the dataset based on the given label.
   *
   * @tparam T The data type of the variable. Defaults to `void`.
   * @tparam R The rank of the variable. Defaults to `mdio::dynamic_rank`.
   * @tparam M The read-write mode of the variable. Defaults to
   * `mdio::ReadWriteMode::dynamic`.
   *
   * @param label The label of the variable to retrieve.
   * @return An `mdio::Result` containing the retrieved variable if successful,
   * or an error if the label is not found or an error occurs during type
   * casting.
   */
  template <typename T = void, DimensionIndex R = dynamic_rank,
            ReadWriteMode M = ReadWriteMode::dynamic>
  Result<Variable<T, R, M>> get(const std::string& label) const {
    if (!variables.count(label)) {
      return absl::NotFoundError("Label '" + label +
                                 "' not found in the stores map");
    }

    auto cast_store =
        tensorstore::StaticCast<tensorstore::TensorStore<T, R, M>>(
            variables.at(label).get_store());

    if (!cast_store.ok()) {
      return cast_store.status();
    }

    return Variable<T, R, M>{variables.at(label).get_variable_name(),
                             variables.at(label).get_long_name(),
                             variables.at(label).getReducedMetadata(),
                             cast_store.value(),
                             variables.at(label).attributes};
  }

  /**
   * Lightweight retrieval of a variable from the dataset based on the given
   * label. Does not provide any type casting. Use get() for type casting.
   *
   * @tparam T The data type of the variable. Defaults to `void`.
   * @tparam R The rank of the variable. Defaults to `mdio::dynamic_rank`.
   * @tparam M The read-write mode of the variable. Defaults to
   * `mdio::ReadWriteMode::dynamic`.
   *
   * @param label The label of the variable to retrieve.
   * @return An `mdio::Result` containing the retrieved variable if successful,
   * or an error if the label is not found or an error occurs during type
   * casting.
   */
  template <typename T = void, DimensionIndex R = dynamic_rank,
            ReadWriteMode M = ReadWriteMode::dynamic>
  Result<Variable<T, R, M>> at(const std::string& label) const {
    if (!variables.count(label)) {
      return absl::NotFoundError("Label '" + label +
                                 "' not found in the stores map");
    }

    return variables.at(label);
  }

  /**
   * @brief Checks if the VariableCollection contains a variable with the
   * specified label.
   * @param label The name of the Variable to check for.
   * @return true if the VariableCollection has that label, false otherwise.
   */
  bool contains_key(const std::string& label) const {
    return variables.count(label) != 0;
  }

  /**
   * @brief Get a vector of all keys in the VariableCollection
   * A helper function to get the keys of all the variables that the
   * VariableCollection contains. Order of the keys is not significant. This is
   * intended for internal use for coordinate retrieval.
   * @return A vector of strings containing the keys of all the variables in the
   * VariableCollection.
   */
  std::vector<std::string> get_keys() const {
    std::vector<std::string> keys;
    // TODO: Is this the most efficient method?
    for (auto& [key, _] : variables) {
      keys.emplace_back(key);
    }
    return keys;
  }

  /**
   * @brief Get a sorted iterable accessor for the VariableCollection
   *
   * Provides a consistent iterable for the VariableCollection.
   * Use this list in conjunction with the at() method to retrieve variables.
   * @return A sorted vector of the keys of all the variables in the
   * VariableCollection.
   */
  std::vector<std::string> get_iterable_accessor() const {
    // The intention here is to hide the std iterators from the user.
    // unordered_map iterators are not guaranteed to have the same order across
    // different compilers.
    std::vector keys = get_keys();
    std::sort(keys.begin(), keys.end());
    return keys;
  }

 private:
  // Define the iterator type for the VariableCollection
  using iterator = std::unordered_map<std::string, Variable<>>::iterator;
  using const_iterator =
      std::unordered_map<std::string, Variable<>>::const_iterator;

  const_iterator begin() const { return variables.cbegin(); }
  const_iterator end() const { return variables.cend(); }
  const_iterator cbegin() const { return variables.cbegin(); }
  const_iterator cend() const { return variables.cend(); }

  std::unordered_map<std::string, Variable<>> variables;
};
}  // namespace mdio
