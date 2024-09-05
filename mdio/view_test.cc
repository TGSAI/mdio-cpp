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

#include "mdio/view.h"
#include "mdio/dataset.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

// clang-format off
#include <nlohmann/json.hpp>  // NOLINT
// clang-format on

namespace {

mdio::Result<mdio::Dataset> createDataset() {
  const std::string path = "view_test.mdio";
  const std::string spec = R"(
{
  "metadata": {
    "name": "WriterStub",
    "apiVersion": "1.0.0",
    "createdOn": "2024-07-11T09:30:00.000000-06:00",
    "attributes": {
      "stubAuthor": "Brian Michell",
      "stubVersion": "0.1.0",
      "primaryKey": "DATA"
    }
  },
  "variables": [
    {
      "name": "WORKER_ID",
      "dataType": "int64",
      "dimensions": [{"name": "WORKER_ID", "size": 4}],
      "metadata": {
        "chunkGrid": {
          "name": "regular",
          "configuration": {
            "chunkShape": [1]
          }
        }
      }
    },
    {
      "name": "TASK_ID",
      "dataType": "int64",
      "dimensions": [{"name": "TASK_ID", "size": 6}],
      "metadata": {
        "chunkGrid": {
          "name": "regular",
          "configuration": {
            "chunkShape": [1]
          }
        }
      }
    },
    {
      "name": "TRACE",
      "dataType": "int64",
      "dimensions": [{"name": "TRACE", "size": 8}],
      "metadata": {
        "chunkGrid": {
          "name": "regular",
          "configuration": {
            "chunkShape": [2]
          }
        }
      }
    },
    {
      "name": "SAMPLE",
      "dataType": "int64",
      "dimensions": [{"name": "SAMPLE", "size": 1}],
      "metadata": {
        "chunkGrid": {
          "name": "regular",
          "configuration": {
            "chunkShape": [1]
          }
        }
      },
      "compressor": {
        "name": "blosc",
        "level": 5
      }
    },
    {
      "name": "INLINE",
      "dataType": "int32",
      "dimensions": ["WORKER_ID", "TASK_ID", "TRACE"],
      "metadata": {
        "chunkGrid": {
          "name": "regular",
          "configuration": {
            "chunkShape": [1, 1, 2]
          }
        }
      }
    },
    {
      "name": "XLINE",
      "dataType": "int32",
      "dimensions": ["WORKER_ID", "TASK_ID", "TRACE"],
      "metadata": {
        "chunkGrid": {
          "name": "regular",
          "configuration": {
            "chunkShape": [1, 1, 2]
          }
        }
      }
    },
    {
      "name": "DATA",
      "dataType": "float32",
      "dimensions": ["WORKER_ID", "TASK_ID", "TRACE", "SAMPLE"],
      "coordinates": ["INLINE", "XLINE"],
      "metadata": {
        "chunkGrid": {
          "name": "regular",
          "configuration": {
            "chunkShape": [1, 1, 2, 1]
          }
        }
      },
      "compressor": {
        "name": "blosc",
        "level": 0
      }
    }
  ]
}
)";
  nlohmann::json j = nlohmann::json::parse(spec);
  auto dsRes = mdio::Dataset::from_json(j, path, mdio::constants::kCreateClean);
  if (!dsRes.status().ok()) {
    return dsRes.status();
  }
  auto ds = dsRes.value();
  MDIO_ASSIGN_OR_RETURN(auto ilVar, ds.variables.get<int>("INLINE"));
    MDIO_ASSIGN_OR_RETURN(auto xlVar, ds.variables.get<int>("XLINE"));
    MDIO_ASSIGN_OR_RETURN(auto dataVar, ds.variables.get<float>("DATA"));

    auto ilDataRes = mdio::from_variable<int>(ilVar);
    if (!ilDataRes.status().ok()) {
        return ilDataRes.status();
    }
    auto xlDataRes = mdio::from_variable<int>(xlVar);
    if (!xlDataRes.status().ok()) {
        return xlDataRes.status();
    }
    auto dataDataRes = mdio::from_variable<float>(dataVar);
    if (!dataDataRes.status().ok()) {
        return dataDataRes.status();
    }

    auto ilData = ilDataRes.value();
    auto xlData = xlDataRes.value();
    auto dataData = dataDataRes.value();

    auto ilAccessor = ilData.get_data_accessor();
    auto xlAccessor = xlData.get_data_accessor();
    auto dataAccessor = dataData.get_data_accessor();

    ilAccessor({0, 0, 0}) = 0;
    ilAccessor({0, 0, 1}) = 0;
    ilAccessor({0, 1, 0}) = 1;
    ilAccessor({0, 1, 1}) = 1;
    ilAccessor({1, 0, 0}) = 2;
    ilAccessor({1, 0, 1}) = 2;
    ilAccessor({1, 1, 0}) = 3;
    ilAccessor({1, 1, 1}) = 3;

    xlAccessor({0, 0, 0}) = 0;
    xlAccessor({0, 0, 1}) = 1;
    xlAccessor({0, 1, 0}) = 0;
    xlAccessor({0, 1, 1}) = 1;
    xlAccessor({1, 0, 0}) = 2;
    xlAccessor({1, 0, 1}) = 3;
    xlAccessor({1, 1, 0}) = 2;
    xlAccessor({1, 1, 1}) = 3;

    dataAccessor({0, 0, 0, 0}) = 0.0f;
    dataAccessor({0, 0, 1, 0}) = 0.1f;
    dataAccessor({0, 1, 0, 0}) = 1.0f;
    dataAccessor({0, 1, 1, 0}) = 1.1f;
    dataAccessor({1, 0, 0, 0}) = 2.2f;
    dataAccessor({1, 0, 1, 0}) = 2.3f;
    dataAccessor({1, 1, 0, 0}) = 3.2f;
    dataAccessor({1, 1, 1, 0}) = 3.3f;

    auto ilFut = ilVar.Write(ilData).result();
    auto xlFut = xlVar.Write(xlData).result();
    auto dataFut = dataVar.Write(dataData).result();

    if (!ilFut.status().ok()) {
        return ilFut.status();
    }
    if (!xlFut.status().ok()) {
        return xlFut.status();
    }
    if (!dataFut.status().ok()) {
        return dataFut.status();
    }

    return ds;
}

TEST(DatasetView, openNoIndex) {
  auto dsRes = createDataset();
  ASSERT_TRUE(dsRes.status().ok()) << dsRes.status();
  auto dsViewRes = mdio::DatasetView::FromDataset(dsRes.value(), mdio::OpenMode::kNoIndex);
  ASSERT_TRUE(dsViewRes.status().ok()) << dsViewRes.status();
}

TEST(DatasetView, openLazy) {
  auto dsRes = createDataset();
  ASSERT_TRUE(dsRes.status().ok()) << dsRes.status();
  auto dsViewRes = mdio::DatasetView::FromDataset(dsRes.value(), mdio::OpenMode::kLazy);
  ASSERT_TRUE(dsViewRes.status().ok()) << dsViewRes.status();
}

TEST(DatasetView, openEager) {
  auto dsRes = createDataset();
  ASSERT_TRUE(dsRes.status().ok()) << dsRes.status();
  auto dsViewRes = mdio::DatasetView::FromDataset(dsRes.value(), mdio::OpenMode::kEager);
  ASSERT_TRUE(dsViewRes.status().ok()) << dsViewRes.status();
}

TEST(DatasetView, openEagerWithSort) {
  auto dsRes = createDataset();
  ASSERT_TRUE(dsRes.status().ok()) << dsRes.status();
  auto dsViewRes = mdio::DatasetView::FromDataset(dsRes.value(), mdio::OpenMode::kEagerWithSort);
  ASSERT_TRUE(dsViewRes.status().ok()) << dsViewRes.status();
}

// TEST(DatasetViewSort, flatHomogeneous) {
//   GTEST_SKIP();
// }

// TEST(DatasetViewSort, flatForward) {
//   GTEST_SKIP();
// }

// TEST(DatasetViewSort, flatReverse) {
//   GTEST_SKIP();
// }

// TEST(DatasetViewSort, chunkHomogeneous) {
//   GTEST_SKIP();
// }

// TEST(DatasetViewSort, chunkForward) {
//   GTEST_SKIP();
// }

// TEST(DatasetViewSort, chunkReverse) {
//   GTEST_SKIP();
// }

TEST(DatasetViewSort, unknown) {
  auto dsRes = createDataset();
  ASSERT_TRUE(dsRes.status().ok()) << dsRes.status();
  
  mdio::RangeDescriptor<> worker = {"WORKER_ID", 0, 2, 1};
  mdio::RangeDescriptor<> task = {"TASK_ID", 2, 6, 1};
  mdio::RangeDescriptor<> trace = {"TRACE", 0, 8, 1};

  auto ds = dsRes.value();
  dsRes = ds.isel(worker, task, trace);
  ASSERT_TRUE(dsRes.status().ok()) << dsRes.status();

  auto dsViewRes = mdio::DatasetView::FromDataset(dsRes.value(), mdio::OpenMode::kLazy);
  ASSERT_TRUE(dsViewRes.status().ok()) << dsViewRes.status();
  auto dsView = dsViewRes.value();
  
  mdio::ValueDescriptor<int> ilValue = {"INLINE", 1};
  std::cout << "Making view" << std::endl;
  auto viewRes = dsView.MakeView("DATA", ilValue);
  std::cout << "View made" << std::endl;
  EXPECT_FALSE(viewRes.status().ok());
}

// TEST(DatasetViewSort, unsorted) {
//   GTEST_SKIP();
// }


}  // namespace