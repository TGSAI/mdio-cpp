/*
WARNING: This file is considered experimental and will change or be removed in future versions.

The current implementation is a work-in-progress hard-coded implementation for accessing a specific dataset schema.

A more generalized implementation is planned for the future.
*/

#include "mdio/mdio.h"

/*
 * Below are constants that are used to identify the Perseus originated dataset
 * according to version 0.1.1 of the Perseus stub.
 */
const static std::string kPerseusStubVersion = "0.1.1";
const static std::string kPerseusPrimaryKey = "primaryKey";
const static std::string kWorkerId = "WORKER_ID";
const static std::string kTaskId = "TASK_ID";
const static std::string kTraceId = "TRACE";
const static std::string kSampleId = "SAMPLE";
/*
 * The _TILE suffix is a construct that will need to be added in-flow by a macro.
 * This should allow for better layout of the data for visualization by distributing it
 * across many tasks while maintaining some locality of similar data.
 */
const static std::string kTileId = "_TILE";

/**
 * @brief Checks if the dataset can be read by this reader.
 * This reader is only intended to read Perseus originated datasets that have been written
 * with the version 0.1.1 Perseus stub, which will be the first version that should be written by the
 * official release of the MDIO_Writer module.
 * @param path The path to the dataset to check.
 * @return The dataset if it can be read, otherwise an error.
 */
mdio::Result<mdio::Dataset> CanRead(const std::string& path, const std::size_t cacheSize = 5000000000) {
    MDIO_ASSIGN_OR_RETURN(auto ds, OpenWithCache(path, cacheSize));
    if (!ds.getMetadata().contains("attributes")) {
        return absl::UnimplementedError("Could not find attributes, meaning this probably did not come from ImagingAnyware.");
    }
    if (!ds.getMetadata()["attributes"].contains(kPerseusPrimaryKey)) {
        return absl::UnimplementedError("Could not find the primary key in metadata. Inference for this is not implemented.");
    }
    if (!ds.getMetadata()["attributes"].contains("stubVersion")) {
        return absl::UnimplementedError("Could not find the stub version in the metadata. Version inference is not implemented.");
    }
    if (ds.getMetadata()["attributes"]["stubVersion"].get<std::string>() != kPerseusStubVersion) {
        return absl::UnimplementedError("Unsupported stub version: " + ds.getMetadata()["attributes"]["stubVersion"].get<std::string>() + ". Only " + kPerseusStubVersion + " is implemented.");
    }
    return ds;
}

/**
 * @brief Opens a dataset with an LRU cache.
 * @param path The path to the dataset to open.
 * @param cacheSize The size of the cache in bytes.
 * @return The dataset if it can be read, otherwise an error.
 */
mdio::Result<mdio::Dataset> OpenWithCache(const std::string& path, const std::size_t cacheSize = 5000000000) {
    auto cacheJson = nlohmann::json::parse(R"({"cache_pool": {"total_bytes_limit": 5000000000}})");
    cacheJson["cache_pool"]["total_bytes_limit"] = cacheSize;
    auto spec = mdio::Context::Spec::FromJson(cacheJson);
    auto ctx = mdio::Context(spec.value());
    auto datasetFut = mdio::Dataset::Open(path, mdio::constants::kOpen, ctx);
    if (!datasetFut.status().ok()) {
        return datasetFut.status();
    }
    return datasetFut.value();
}

/**
 * @brief A struct holding the keys that are searchable.
 * @param taskKeys The task keys in their sort order.
 * @param sortKeys The sort keys in their sort order.
 * @param promotedKeys The promoted keys in their sort order.
 */
struct SearchableKeys {

    std::tuple<std::set<std::string>, std::set<std::string>, std::set<std::string>> toSet() {
        std::set<std::string> tK;
        std::set<std::string> sK;
        std::set<std::string> pK;
        for (const auto& key : taskKeys) {
            tK.insert(key);
        }
        for (const auto& key : sortKeys) {
            sK.insert(key);
        }
        for (const auto& key : promotedKeys) {
            pK.insert(key.key);
        }
        return std::make_tuple(tK, sK, pK);
    }

    std::vector<std::string> taskKeys;
    std::vector<std::string> sortKeys;
    struct PromotedKey {
        std::string key;
        std::string sortKey;
    };
    std::vector<PromotedKey> promotedKeys;
};

/**
 * @brief Gets the task key names. These are the searchable coordinates.
 * @param ds The dataset to search.
 * @return A pair of sets of keys. The first is the task keys, the second is the sort keys.
 */
mdio::Result<SearchableKeys> GetSearchableKeys(mdio::Dataset& ds) {
    SearchableKeys searchableKeys;
    /*
     * Only supporting Perseus originated makes this easy.
     * In the future, we will need to update this to support primary key inference or user specified keys.
     * Likewise, as we add support for multiple vector fields, we will have to update this to support multiple primary keys.
     */
    MDIO_ASSIGN_OR_RETURN(auto dataVar, ds.variables.at(ds.getMetadata()["attributes"][kPerseusPrimaryKey].get<std::string>()));
    auto varMetadata = dataVar.getMetadata();
    // The coordinates are arbitrary order space delmimited strings.
    std::vector<std::string> coords = absl::StrSplit(varMetadata["coordinates"].get<std::string>(), ' ');
    for (const std::string& coord : coords) {
        MDIO_ASSIGN_OR_RETURN(auto coordVar, ds.variables.at(coord));
        if (coordVar.rank() == 2) {
            searchableKeys.taskKeys.push_back(coord);
        } else if (coordVar.rank() == 3) {
            searchableKeys.sortKeys.push_back(coord);  // This is the Trace_Index value for LT View settings (I think?)
        } else {
            return absl::InvalidArgumentError("Expected 2D or 3D coordinate but got " + coord);
        }
    }
    auto [taskKeys, _, __] = searchableKeys.toSet();
    for (const auto& key : taskKeys) {
        if (key.length() > kTileId.length() && key.substr(key.length() - kTileId.length()) == kTileId) {
            searchableKeys.promotedKeys.push_back({key, key.substr(0, key.length() - kTileId.length())});
        }
    }

    // Add the SAMPLE dimension as the final view key
    searchableKeys.sortKeys.emplace_back(kSampleId);
    auto dataVarMeta = dataVar.GetAttributes();
    if (dataVarMeta.contains("attributes")) {
        if (dataVarMeta["attributes"].contains("fieldType")) {
            std::cout << "The field type for " << kSampleId << " is " << dataVarMeta["attributes"]["fieldType"].get<std::string>();
            if (dataVarMeta["attributes"]["fieldType"].get<std::string>() == "DEPTH_SERIES") {
                std::cout << " and the units are " << dataVarMeta["attributes"]["units"].get<std::string>() << std::endl;
            } else if (dataVarMeta["attributes"]["fieldType"].get<std::string>() == "TIME_SERIES") {
                std::cout << " and the units are ms" << std::endl;
            } else {
                std::cout << " and the units are unknown" << std::endl;
            }
        } else {
            std::cout << dataVarMeta.dump(4) << std::endl;
        }
    } else {
        std::cout << dataVarMeta.dump(4) << std::endl;
    }

    return searchableKeys;
}

/**
 * @brief Prints the values of the searchable keys.
 * @param ds The dataset to print the values of.
 * @param searchableKeys The searchable keys to print the values of.
 */
mdio::Result<void> PrintSearchableValues(mdio::Dataset& ds, SearchableKeys& searchableKeys) {
    for (const auto& key : searchableKeys.taskKeys) {
        MDIO_ASSIGN_OR_RETURN(auto dimVar, ds.variables.get<PrimaryKeyType>(key));
        auto dimDataFuture = dimVar.Read();
        if (!dimDataFuture.status().ok()) {
            return dimDataFuture.status();
        }
        auto dimData = dimDataFuture.value();
        for (auto i=0; i<dimVar.get_num_samples(); ++i) {
            std::cout << key << ": " << dimData.get_data_accessor().data()[i] << std::endl;
        }
    }
    return absl::OkStatus();
}

/**
 * @brief Searches the live mask for the last true value.
 * @param liveMaskAccessor The live mask accessor to search.
 * @param i The i index to search (WORKER_ID).
 * @param j The j index to search (TASK_ID).
 * @param maxK The maximum k index to search (TRACE).
 * @return The last true value in the live mask.
 * This has an edge case where the 0th element is true, but the return 0 is interpreted as no live values.
 * We don't want to return an error though.
 */
mdio::Result<mdio::Index> SearchLiveMask(mdio::SharedArray<mdio::dtypes::bool_t, mdio::dynamic_rank, mdio::offset_origin>& liveMaskAccessor, const ptrdiff_t offset3D, const mdio::Index maxK) {
    if (!liveMaskAccessor.data()[offset3D]) {
        return 0;
    }
    for (auto k=maxK-1; k>=0; --k) {
        if (liveMaskAccessor.data()[offset3D + k]) {
            return k;
        }
    }
    return absl::NotFoundError("An unexpected error occurred while searching the live mask.");
}

template <typename PrimaryKeyType, typename SecondaryKeyType, typename VectorFieldType>
using CoordinateFuturePair3D_async = std::vector<std::tuple<mdio::Future<mdio::VariableData<PrimaryKeyType>>, mdio::Future<mdio::VariableData<SecondaryKeyType>>, mdio::Future<mdio::VariableData<VectorFieldType>>>>;

template <typename PrimaryKeyType, typename SecondaryKeyType, typename VectorFieldType>
using CoordinateFuturePair3D_resolved = std::vector<std::tuple<mdio::VariableData<PrimaryKeyType>, mdio::VariableData<SecondaryKeyType>, mdio::VariableData<VectorFieldType>>>;

/**
 * @brief Handles the search and sort of 4-dimensional datasets and transposes it back to its natural domain.
 * Natural domain could be Inline/Xline or Channel/FFID, for example.
 * @param ds The dataset to search and sort.
 * @param taskKeys The task keys to search for. In this case, the task keys are the coordinate values that will be searched and sorted by.
 * @param primaryKey The sort key to search for. 
 * @return A vector of tuples of the task keys and the vector field data.
 */
template <typename PrimaryKeyType, typename SecondaryKeyType, typename VectorFieldType>
mdio::Result<CoordinateFuturePair3D_resolved<PrimaryKeyType, SecondaryKeyType, VectorFieldType>> SearchSortTranspose(mdio::Dataset& ds, std::vector<std::pair<std::string, PrimaryKeyType>>& taskKeys, std::string& primaryKey, std::string& secondaryKey) {
    // We make a copy so we can potentially z-slice it
    mdio::Dataset searchableDs = ds;

    /*
     * My only current sample data which would use full sorting of the dataset is with z-slicing.
     * Below is a commented out example of how I would go about checking that a full-sort is required.
     * Having this enabled caused compile time issues though, because things were strictly typed.
     * We can get around that by using the `.at(std::string)` method, or handling multiple dtypes with
     * whatever method is most appropriate for the use case to correctly set the template.
     * 
     * I would recommend using the `.at(std::string)` method unless you intend to read the actual data to
     * determine what kind of sorting is required (it is possible that no sort is actually required, for
     * instance, if the step is 0 because the data is uniform across the task).)
     */
    bool doFullSort = (secondaryKey == kSampleId);

    /*
     * We could update this to use `doFullSort`, but I want to keep it explicit that this behavior
     * is intended explicitly for z-slicing.
     */
    if (secondaryKey != kSampleId) {
        mdio::dtypes::int64_t sampleValue;
        for (const auto& [key, value] : taskKeys) {
            if (key == kSampleId) {
                sampleValue = static_cast<mdio::dtypes::int64_t>(value);
                break;
            }
        }   
        mdio::ValueDescriptor<mdio::dtypes::int64_t> sampleSlice = {kSampleId, sampleValue};
        MDIO_ASSIGN_OR_RETURN(searchableDs, ds.sel(sampleSlice));
    }


    /*
     * All data written with the current Perseus stub (version 0.1.1) must have a primary key.
     * This key is the vector field that is written to the dataset.
     * In the MDIO_Writer module, this will be equal to the Data field parameter.
     */
    MDIO_ASSIGN_OR_RETURN(auto dataVar, searchableDs.variables.get<VectorFieldType>(searchableDs.getMetadata()["attributes"][kPerseusPrimaryKey].get<std::string>()));
    MDIO_ASSIGN_OR_RETURN(auto headersVar, searchableDs.variables.get<mdio::dtypes::byte_t>("headers"));


    if (secondaryKey != kSampleId) {
        MDIO_ASSIGN_OR_RETURN(auto chunkingSchema, dataVar.get_chunk_shape());
        if (chunkingSchema[3] > 128) {
            /*
             * 128 is a somewhat arbitrary number, but if we are targeting an 8MiB chunk size it is unlikely that
             * being chunked so large in the sample dimension is an optimal configuration. We will likely be
             * discarding a lot of data and killing the cache performance by evicting chunks from the cache too quickly.
             * 
             * In my testing with the current sorting as well, the performance suffered slightly with larget chunk sizes in
             * the sample dimension for z-slicing.
            */
            std::cout << "WARNING: This volume does not appear to be optimized for z-slicing. This may result in very poor performance and may fail to load." << std::endl;
        }
    }

    /*
     * The live_mask Variable must always be present in the current version of the Perseus stub (version 0.1.1).
     * It determines where the written data is actually live and not just padded fill values (0's or NaNs depending on the dtype).
     * The data is written densely in the TRACE dimension, and once the live mask goes false, that must be the end of that worker/task 
     * data.
     */
    MDIO_ASSIGN_OR_RETURN(auto liveMaskVar, searchableDs.variables.get<mdio::dtypes::bool_t>("live_mask"));
    /*
     * Reading the full live mask may become problematic for large datasets.
     * It is equal to the number of traces in the dataset, as well as any additonal padding
     * in the TRACE dimension (trace_dimension_chunking in the IA module).
     */
    auto liveMaskFuture = liveMaskVar.Read();

    MDIO_ASSIGN_OR_RETURN(auto primaryVar, searchableDs.variables.get<PrimaryKeyType>(primaryKey));
    MDIO_ASSIGN_OR_RETURN(auto secondaryVar, searchableDs.variables.get<SecondaryKeyType>(secondaryKey));

    /*
     * Reading all of the task keys has been a fairly low overhead operation in my testing.
     * This could become an issue dealing with large prestack datasets.
     * We could reduce the memory overhead by slicing the task keys on the WORKER_ID/TASK_ID loops.
     * This will add some additional latency to every search.
     * We could also update the search algorithm to search each task key individually along the full dataset.
     * We would have to be careful to cache the coordinate values to avoid re-reading them.
     */
    std::vector<mdio::Future<mdio::VariableData<PrimaryKeyType>>> dimFutures;
    for (const auto& coord : taskKeys) {
        if (coord.first == kSampleId && secondaryKey != kSampleId) {
            continue;
        }
        MDIO_ASSIGN_OR_RETURN(auto dimVar, searchableDs.variables.get<PrimaryKeyType>(coord.first));
        dimFutures.push_back(dimVar.Read());
    }

    /*
     * We use the live mask Variable to get the intervals because it's readily available
     * If we need to start slicing it, we must make sure to get this value at the beginning of the method,
     * or ensure that it still aligns with the search domain.
     */
    MDIO_ASSIGN_OR_RETURN(auto intervals, liveMaskVar.get_intervals());

    /*
     * This is the logical domain of the dataset as output by Perseus. These MUST always be present in the current version of the
     * Perseus stub (current version 0.1.1).
     * We don't want the user to ever actually view based on, or even see, these values.
     * The one exception would be the SAMPLE dimension, which could be depth, time, or possibly something else 
     * that is made available by Perseus.
     */
    mdio::RangeDescriptor<> workerDesc = {kWorkerId, intervals[0].inclusive_min, intervals[0].inclusive_min+1, 1};
    mdio::RangeDescriptor<> taskDesc = {kTaskId, intervals[1].inclusive_min, intervals[1].inclusive_min+1, 1};
    mdio::RangeDescriptor<> traceDesc = {kTraceId, intervals[2].inclusive_min, intervals[2].inclusive_min+1, 1};
    
    if (!liveMaskFuture.status().ok()) {
        return liveMaskFuture.status();
    }
    auto liveMaskData = liveMaskFuture.value().get_data_accessor();

    std::vector<mdio::VariableData<PrimaryKeyType>> dimData;
    for (auto& future : dimFutures) {
        if (!future.status().ok()) {
            return future.status();
        }
        dimData.push_back(future.value());
    }

    /*
     * I, J, and K are the logical extents of the dataset.
     * See above discussion regarding logical domain.
     * We will only be using K as a starting point for the live mask search.
     */
    mdio::Index I = intervals[0].exclusive_max - intervals[0].inclusive_min;
    mdio::Index J = intervals[1].exclusive_max - intervals[1].inclusive_min;
    mdio::Index K = intervals[2].exclusive_max - intervals[2].inclusive_min;

    CoordinateFuturePair3D_async<PrimaryKeyType, SecondaryKeyType, VectorFieldType> futurePair;
    /*
     * We want to hold our sliced Variables in memory outside of the loop to ensure
     * that they do not go out of scope too early.
     * This is a known issue with the MDIO library where the string gets deleted pre-maturely.
    */
    std::vector<mdio::Variable<VectorFieldType>> slicedVars;
    std::vector<mdio::Variable<PrimaryKeyType>> slicedPrimaryVars;
    std::vector<mdio::Variable<SecondaryKeyType>> slicedSecondaryVars;
    ptrdiff_t offset2D = 0;
    ptrdiff_t offset3D = 0;
    std::size_t taskKeySize = taskKeys.size();

    for (mdio::Index worker=0; worker<I; ++worker) {
        auto workerStart = std::chrono::high_resolution_clock::now();
        size_t rangesFound = 0;
        size_t matchesFound = 0;

        // Pre-allocate vectors to avoid reallocations
        std::vector<mdio::Index> matchingIndices;
        std::vector<mdio::Index> currentMatches;
        std::vector<mdio::Index> intersection;
        matchingIndices.reserve(K);
        currentMatches.reserve(K);
        intersection.reserve(K);

        for (mdio::Index task = 0; task < J; ++task) {
            // k is the “live” size for the current (worker, task) and is <= uppercase K.
            MDIO_ASSIGN_OR_RETURN(auto k, SearchLiveMask(liveMaskData, offset3D, K));
            /*
             * An important note is that the data returned from the `.get_data_accessor()({})` method is the
             * same as the value returned from the `.get_data_accessor().data()[]` method, however using the
             * `.data()` method is significantly faster.
             * We do need to be very careful about using an appropriate offset when accessing the data.
             * For our case, since we are searching the full extents of the dataset, we can safely start at
             * an offset of 0 and increment by the known chunk size. If we had sliced the dataset along the
             * worker/task/trace domain before searching, we would need to use the `.get_flattened_offset()`
             * method instead of initializing to zero.
             * 
             * Since the array is flattened, we only need the index offset, even if we have sliced the dataset
             * across multiple dimensions. We do not need to account for multiple offsets within the array.
             */
            if (!liveMaskData.data()[offset3D]) {
                ++offset2D;
                offset3D += K;
                continue;
            }
            bool found = true;
            for (size_t i = 0; i < taskKeySize; ++i) {
                if (taskKeys[i].first == kSampleId && secondaryKey != kSampleId) {
                    /*
                     * We want to skip searching the sample dimension if we are z-slicing.
                     */
                    continue;
                }
                if (dimData[i].rank() == 2) {
                    if (dimData[i].get_data_accessor().data()[offset2D] != taskKeys[i].second) {
                        found = false;
                        break;
                    }
                }
            }
            if (!found) {
                // continue to next task
                ++offset2D;
                offset3D += K;
                continue;
            }

            // Find indices where all rank 3 dimensions match
            matchingIndices.clear();
            bool firstRank3 = true;

            for (size_t i = 0; i < taskKeySize; ++i) {
                if (taskKeys[i].first == kSampleId && secondaryKey != kSampleId) {
                    // We want to skip searching the sample dimension if we are z-slicing.
                    continue;
                }
                if (dimData[i].rank() == 3) {
                    auto accessor = dimData[i].get_data_accessor();
                    currentMatches.clear();
                    
                    // Find all matching indices for this dimension
                    for (mdio::Index idx = 0; idx < k; ++idx) {
                        if (accessor.data()[offset3D + idx] == taskKeys[i].second) {
                            currentMatches.push_back(idx);
                        }
                    }

                    if (firstRank3) {
                        matchingIndices = currentMatches;  // Copy instead of move since we reuse currentMatches
                        firstRank3 = false;
                    } else {
                        intersection.clear();
                        // Both vectors are already sorted by construction
                        std::set_intersection(
                            matchingIndices.begin(), matchingIndices.end(),
                            currentMatches.begin(), currentMatches.end(),
                            std::back_inserter(intersection)
                        );
                        matchingIndices.swap(intersection);  // Efficient swap instead of move
                    }

                    if (matchingIndices.empty()) {
                        break;
                    }
                }
            }
            offset3D += K;
            workerDesc.start = worker;
            workerDesc.stop = worker + 1;
            taskDesc.start = task;
            taskDesc.stop = task + 1;

            // Process matching indices into contiguous ranges
            if (!matchingIndices.empty()) {
                matchesFound += matchingIndices.size();
                
                // More efficient range creation with a single pass
                std::vector<std::pair<mdio::Index, mdio::Index>> ranges;
                ranges.reserve(matchingIndices.size() / 2 + 1);  // Worst case scenario
                
                mdio::Index rangeStart = matchingIndices[0];
                mdio::Index prevIdx = matchingIndices[0];

                for (size_t i = 1; i < matchingIndices.size(); ++i) {
                    if (matchingIndices[i] != prevIdx + 1) {
                        // End of continuous range
                        ranges.emplace_back(rangeStart, prevIdx + 1);
                        rangeStart = matchingIndices[i];
                    }
                    prevIdx = matchingIndices[i];
                }
                // Add the last range
                ranges.emplace_back(rangeStart, prevIdx + 1);
                rangesFound += ranges.size();

                // Create slices for each continuous range
                // Pre-size containers to avoid reallocations
                slicedVars.reserve(slicedVars.size() + ranges.size());
                slicedPrimaryVars.reserve(slicedPrimaryVars.size() + ranges.size());
                slicedSecondaryVars.reserve(slicedSecondaryVars.size() + ranges.size());

                
                for (const auto& [start, stop] : ranges) {
                    traceDesc.start = start;
                    traceDesc.stop = stop;

                    MDIO_ASSIGN_OR_RETURN(auto slicedTrace, dataVar.slice(workerDesc, taskDesc, traceDesc));
                    MDIO_ASSIGN_OR_RETURN(auto primarySlice, primaryVar.slice(workerDesc, taskDesc, traceDesc));
                    MDIO_ASSIGN_OR_RETURN(auto secondarySlice, secondaryVar.slice(workerDesc, taskDesc, traceDesc));
                    MDIO_ASSIGN_OR_RETURN(auto headersSlice, headersVar.slice(workerDesc, taskDesc, traceDesc));
                    slicedVars.push_back(slicedTrace);
                    slicedPrimaryVars.push_back(primarySlice);
                    slicedSecondaryVars.push_back(secondarySlice);
                    futurePair.emplace_back(std::make_tuple(primarySlice.Read(), secondarySlice.Read(), slicedTrace.Read()));
                }
            } else if (matchingIndices.empty() && dimData.size() == 0) {
                // Case where we are viewing 3-D data in the time domain.
                /*
                 * We will just grab the full extents of the available dataset in respect to the selected view.
                 */
                traceDesc.start = 0;
                traceDesc.stop = k+1;  // Lowercase k, we want to grab all live traces in the chunk.

                MDIO_ASSIGN_OR_RETURN(auto slicedTrace, dataVar.slice(workerDesc, taskDesc, traceDesc));
                MDIO_ASSIGN_OR_RETURN(auto primarySlice, primaryVar.slice(workerDesc, taskDesc, traceDesc));
                MDIO_ASSIGN_OR_RETURN(auto secondarySlice, secondaryVar.slice(workerDesc, taskDesc, traceDesc));
                MDIO_ASSIGN_OR_RETURN(auto headersSlice, headersVar.slice(workerDesc, taskDesc, traceDesc));
                slicedVars.push_back(slicedTrace);
                slicedPrimaryVars.push_back(primarySlice);
                slicedSecondaryVars.push_back(secondarySlice);
                futurePair.emplace_back(std::make_tuple(primarySlice.Read(), secondarySlice.Read(), slicedTrace.Read()));
            }
        }  // End of task loop
        
    }  // End of worker loop

    CoordinateFuturePair3D_resolved<PrimaryKeyType, SecondaryKeyType, VectorFieldType> resolvedFuturePair;

    // First resolve all futures
    for (auto& future : futurePair) {
        if (!std::get<0>(future).status().ok()) {
            return std::get<0>(future).status();
        }
        if (!std::get<1>(future).status().ok()) {
            return std::get<1>(future).status();
        }
        if (!std::get<2>(future).status().ok()) {
            return std::get<2>(future).status();
        }
        resolvedFuturePair.emplace_back(std::make_tuple(
            std::get<0>(future).value(),
            std::get<1>(future).value(),
            std::get<2>(future).value()
        ));
    }

    // We're done with the slicedVars now.
    slicedVars.clear();
    slicedPrimaryVars.clear();
    slicedSecondaryVars.clear();

    /*
     * For the case where we are z-slicing, we need to perform a fairly extensive sort.
     * In this example we will break up the data into its atomic elements and sort based
     * on the primary and secondary keys.
     * The sorting will happen in-place and the resolvedFuturePair will be updated to reflect
     * the now sorted data.
     * 
     * This is not the most efficient or elegant. I'm positive there are many avenues for optimization
     * but that would make the example more complex.
    */
    if (secondaryKey != kSampleId) {
        // First, flatten all the data into vectors of triplets
        struct SortableUnit {
            PrimaryKeyType primaryKey;
            SecondaryKeyType secondaryKey;
            VectorFieldType data;
            size_t originalTupleIndex;
            size_t positionInTuple;
        };
        
        std::vector<SortableUnit> allUnits;
        
        // Collect all units from all tuples
        for (size_t tupleIdx = 0; tupleIdx < resolvedFuturePair.size(); ++tupleIdx) {
            auto& tuple = resolvedFuturePair[tupleIdx];
            auto& pKey = std::get<0>(tuple);
            auto& sKey = std::get<1>(tuple);
            auto& vKey = std::get<2>(tuple);
            
            auto pData = pKey.get_data_accessor().data() + pKey.get_flattened_offset();
            auto sData = sKey.get_data_accessor().data() + sKey.get_flattened_offset();
            auto vData = vKey.get_data_accessor().data() + vKey.get_flattened_offset();
            
            for (size_t i = 0; i < pKey.num_samples(); ++i) {
                allUnits.push_back({
                    pData[i],
                    sData[i],
                    vData[i],
                    tupleIdx,
                    i
                });
            }
        }
        
        // Sort all units
        std::sort(allUnits.begin(), allUnits.end(),
            [](SortableUnit& a, SortableUnit& b) {
                if (a.primaryKey != b.primaryKey) {
                    return a.primaryKey < b.primaryKey;
                }
                return a.secondaryKey < b.secondaryKey;
            });
            
        // Reconstruct the tuples with sorted data
        std::vector<std::tuple<mdio::VariableData<PrimaryKeyType>, 
                             mdio::VariableData<SecondaryKeyType>,
                             mdio::VariableData<VectorFieldType>>> newPairs;
                             
        // Group sorted units back into tuples of the original sizes
        size_t currentPos = 0;
        for (size_t tupleIdx = 0; tupleIdx < resolvedFuturePair.size(); ++tupleIdx) {
            auto& originalTuple = resolvedFuturePair[tupleIdx];
            size_t tupleSize = std::get<0>(originalTuple).num_samples();
            
            // Create new arrays for this tuple
            std::vector<PrimaryKeyType> newPrimary(tupleSize);
            std::vector<SecondaryKeyType> newSecondary(tupleSize);
            std::vector<VectorFieldType> newData(tupleSize);
            
            // Fill with sorted data
            for (size_t i = 0; i < tupleSize; ++i) {
                newPrimary[i] = allUnits[currentPos + i].primaryKey;
                newSecondary[i] = allUnits[currentPos + i].secondaryKey;
                newData[i] = allUnits[currentPos + i].data;
            }
            currentPos += tupleSize;
            
            // Create new VariableData objects with sorted data
            auto pKey = std::get<0>(originalTuple);
            auto sKey = std::get<1>(originalTuple);
            auto vKey = std::get<2>(originalTuple);
            
            // Copy sorted data back to original accessors
            std::copy(newPrimary.begin(), newPrimary.end(), 
                     pKey.get_data_accessor().data() + pKey.get_flattened_offset());
            std::copy(newSecondary.begin(), newSecondary.end(), 
                     sKey.get_data_accessor().data() + sKey.get_flattened_offset());
            std::copy(newData.begin(), newData.end(), 
                     vKey.get_data_accessor().data() + vKey.get_flattened_offset());
            
            newPairs.push_back(std::make_tuple(pKey, sKey, vKey));
        }
        
        // Replace original pairs with sorted ones
        resolvedFuturePair.clear();
        resolvedFuturePair = std::move(newPairs);
    } else {
        // Original sort for time/sample domain
        std::sort(resolvedFuturePair.begin(), resolvedFuturePair.end(), [](auto& a, auto& b) {
            auto accessorA = std::get<0>(a).get_data_accessor();
            auto accessorB = std::get<0>(b).get_data_accessor();
            auto accessorA2 = std::get<1>(a).get_data_accessor();
            auto accessorB2 = std::get<1>(b).get_data_accessor();
            
            auto valueA1 = accessorA.data()[std::get<0>(a).get_flattened_offset()];
            auto valueB1 = accessorB.data()[std::get<0>(b).get_flattened_offset()];
            auto valueA2 = accessorA2.data()[std::get<1>(a).get_flattened_offset()];
            auto valueB2 = accessorB2.data()[std::get<1>(b).get_flattened_offset()];
            
            // If first values are different, sort by them
            if (valueA1 != valueB1) {
                return valueA1 < valueB1;
            }
            // If first values are equal, sort by second values
            return valueA2 < valueB2;
        });
    }

    return resolvedFuturePair;
}
