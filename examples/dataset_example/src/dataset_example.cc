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

/**
 * @file Provides an example of the basic usage of an MDIO Dataset.
 * Goals:
 * 1. Starting with an examlpe dataset, explore the variables and metadata.
 * 2. Understand the in memory SharedArray and on disk representations of the
 * data.
 * 3. Read and write with a slicing operator.
 * 4. Understand Future and Result operators.
 * library.
 */

#include "dataset_example.h"

#include <filesystem>

#include "mdio/dataset.h"

using Index = mdio::Index;
using namespace mdio::dtypes;

/**
 * @brief This provides a basic MDIO example, covering its API usage and key
 * concepts.
 */
absl::Status Run() {
  /// Datasets create a number of zarr "files" which are directories
  /// with json metadata.
  std::filesystem::path temp_dir = std::filesystem::temp_directory_path();

  std::string dataset_path = temp_dir / "zarrs/seismic_3d/";

  auto json_spec = GetToyExample();

  /// A dataset is used to represent a collection of data "on disk".
  ///
  /// The MDIO json schema for this can be found here:
  ///    mdio/dataset_schema.h
  ///
  /// Open Options handle these use cases.
  ///
  /// mdio::constants::kOpen - Open an existing dataset.
  ///
  /// mdio::constants::kCreate -
  ///    Create a new dataset, but don't overwrite and existing if
  ///    one exists in the path.
  ///
  /// mdio::constants::kCreateClean -
  ///    Create a new dataset and overwrite an existing one.
  ///
  auto dataset_future = mdio::Dataset::from_json(json_spec, dataset_path,
                                                 mdio::constants::kCreateClean);

  /// MDIO Datasets are compatible with Python's Xarray format:
  /// https://docs.xarray.dev/en/stable/
  /// The can be created or loaded using Xarray.

  /// Creating a new dataset will write json data to disk. There maybe some
  /// latency in doing these write, particularly to object stores. The return
  /// type of this operation is a Tensorstore Future. A Future allows this I/O
  /// to happen without blocking the main thread.
  auto dataset_result = dataset_future.result();
  /// Here result() Blocks so we have result for the next step.

  /// The MDIO library is not intended to throw exceptions, instead errors are
  /// handled by returning a "Result" object; the result can be tested for
  /// "ok()", there is also a convenient Tensorstore macro for doing assigment
  /// while handling errors.
  MDIO_ASSIGN_OR_RETURN(auto dataset, dataset_result)

  /// The dataset represent data on disk, the object holds only minimal state.
  /// Its memory footprint is small.
  std::cout << dataset << "\n" << std::endl;

  /// A dataset represents a collection of variables, and these variable can be
  /// accessed by name. The type and other information is discoverable at
  /// runtime, initially the variable will have a "void" type.
  std::cout << "dtype of the velocity : "
            << dataset.variables.at("velocity")->dtype() << "\n"
            << std::endl;

  /// Collections of metadata including units can be accessed from the variable.
  std::cout << "metadata of the velocity : "
            << dataset.variables.at("velocity")->getMetadata() << "\n"
            << std::endl;

  /// Struct data in this context is a little different because unless a "field"
  /// is requests by tensorstore, it will be opening as a byte array.
  std::cout << "\n dtype of the velocity image_headers : "
            << dataset.variables.at("image_headers")->dtype() << "\n"
            << std::endl;

  /// At this point however, we have just initialize the Dataset, we have not
  /// assigned any values.

  /// Seismic datasets are very large and can't typically be loaded at once
  /// into memory.
  /// Using a dataset, subsets of data can be sliced. Here is an example,
  /// the object is identifier, start, stop, step
  mdio::SliceDescriptor desc1 = {"inline", 20, 120, 1};
  mdio::SliceDescriptor desc2 = {"crossline", 100, 200, 1};

  MDIO_ASSIGN_OR_RETURN(auto slice, dataset.isel(desc1, desc2))
  /// The slice represents a subset of the MDIO data on disk, I/O operations can
  /// be made using the variables contained in this slice.
  std::cout << slice << "\n\n" << std::endl;

  /// Each Variable object in the dataset is responsible for it own I/O, to load
  /// data into memory or write data to disk, use the Variable. MDIO is a self
  /// describing data, this means that the datatype is discovered at runtime.
  /// Here we request a Variable given we know its dtype of unit32:
  MDIO_ASSIGN_OR_RETURN(auto variableObject,
                        slice.variables.get<uint32_t>("inline"))
  /// The Variable object contains a collection of metadata associated with the
  /// seismic, including names, and units.
  std::cout << variableObject << std::endl;

  /// I/O operations can be handled by the tensorstore API applied to the store
  /// member of the Variable, which also provides a rich set of tensor
  /// operations. One feature of tensorstore is that it handles its domain on
  /// slice, since we sliced inline index 20, 120, we need to make sure we
  /// address that range ...
  auto inclusive_min =
      variableObject.get_store().domain()[0].interval().inclusive_min();
  auto exclusive_max =
      variableObject.get_store().domain()[0].interval().exclusive_max();

  /// Reads in tensorstore are asynchronous, we use the "result()" method to
  /// await the read.
  // MDIO_ASSIGN_OR_RETURN(
  //   auto data, tensorstore::Read(variableObject.get_store()).result()
  // )
  MDIO_ASSIGN_OR_RETURN(auto data, variableObject.Read().result())
  auto d1 = data.get_data_accessor();
  /// The read returns a SharedArray, which we can addrss like and array, in 3-d
  /// we might do something like this data({0, 0, 0}), here the inline label is
  /// 1-d.
  for (Index i = inclusive_min; i < exclusive_max; ++i) {
    // data(i) = i*10 + 1001;
    d1(i) = i * 10 + 1001;
  }

  /// SharedArray's can be written to the disk using the store object.
  // auto write_result = tensorstore::Write(
  //   data, variableObject.get_store()
  // ).result();
  auto write_result = variableObject.Write(data).result();
  std::cout << "variable write is : " << write_result.status() << std::endl;

  /// Optionally, the variable can handle the I/O. In this scenario data can be
  /// read into a VariableData object that retains it's dimension names and
  /// other metadata.
  MDIO_ASSIGN_OR_RETURN(auto variableData, variableObject.Read().result())

  /// The accessor method provides access to an underlying tensorstore
  /// SharedArray.
  auto tick_labels = variableData.get_data_accessor();
  for (Index i = inclusive_min; i < exclusive_max; i += 20) {
    std::cout << variableData.variableName << " , index  " << i
              << " , value = " << tick_labels(i) << std::endl;
    tick_labels(i) += 1000;
  }
  auto variableWriteFuture = variableObject.Write(variableData).result();
  std::cout << "variableWriteFuture ... " << variableWriteFuture.status()
            << "\n\n"
            << std::endl;

  /// An existing Dataset can also be read from disk given its path. Metadata is
  /// read from a consolidated ".zmetadata" json which written when the dataset
  /// was first created. Dataset metadata is static, we do not intend for the
  /// use to be able to change it other than creating a new dataset.
  auto existing_dataset =
      mdio::Dataset::Open(dataset_path, mdio::constants::kOpen).result();

  MDIO_ASSIGN_OR_RETURN(dataset, existing_dataset)

  /// In the previous scenario used a slice to operate over the range of data
  /// [20, 120), this dataset is defined over the entire range, so we should see
  /// our changes on the subdomain, and the fill value "0" otherwise.
  ///
  /// In this case we know the data type of the variable, the get<> method will
  /// cast the result appropriately. In case the underlying data is not castable
  /// to uint32_t (in this case), then the get method will return a result that
  /// is not OK.
  MDIO_ASSIGN_OR_RETURN(variableObject,
                        dataset.variables.get<uint32_t>("inline"))
  inclusive_min =
      variableObject.get_store().domain()[0].interval().inclusive_min();
  exclusive_max =
      variableObject.get_store().domain()[0].interval().exclusive_max();

  /// Here we read all of the variable from disk into memory
  MDIO_ASSIGN_OR_RETURN(variableData, variableObject.Read().result())
  tick_labels = variableData.get_data_accessor();
  for (Index i = inclusive_min; i < exclusive_max; i += 20) {
    std::cout << "dataset domain, " << variableData.variableName
              << " +1000, index  " << i << " , value = " << tick_labels(i)
              << std::endl;
    tick_labels(i) += 1000;
  }

  // An example populating values for the inline/crossline coordinates ...
  for (const auto& label : dataset.domain.labels()) {
    if (label.empty()) {
      continue;
    }
    MDIO_ASSIGN_OR_RETURN(
        /// in this example, all the dims labels are unint32
        variableObject, dataset.variables.get<uint32_t>(label))

    /// Suppose we want to read existing values ...
    MDIO_ASSIGN_OR_RETURN(variableData, variableObject.Read().result())

    inclusive_min =
        variableObject.get_store().domain()[0].interval().inclusive_min();
    exclusive_max =
        variableObject.get_store().domain()[0].interval().exclusive_max();

    tick_labels = variableData.get_data_accessor();
    for (Index i = inclusive_min; i < exclusive_max; ++i) {
      tick_labels(i) += i;
    }
    variableWriteFuture = variableObject.Write(variableData).result();
    if (!variableWriteFuture.ok()) {
      return variableWriteFuture.status();
    }
  }

  /// For the purposes of information hiding, you can also extract a single
  /// variable and its coordinates, with the other metadata. This acts like a
  /// regular Dataset, but has only a single variable.
  MDIO_ASSIGN_OR_RETURN(auto inline_labels, dataset["cdp-x"])

  return absl::OkStatus();
}

int main() {
  auto status = Run();
  if (!status.ok()) {
    std::cout << "Task failed.\n" << status << std::endl;
  } else {
    std::cout << "MDIO Example Complete.\n";
  }
  return status.ok() ? 0 : 1;
}