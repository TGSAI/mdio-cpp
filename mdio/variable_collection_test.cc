#include "variable_collection.h"
#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace {

// clang-format off
::nlohmann::json json_good = {
    {"driver", "zarr"},
    {"kvstore",
        {
            {"driver", "file"},
            {"path", "collectionVariable"}
        }
    },
    {"attributes", 
        {
            {"long_name", "foooooo ....."}, // required
            {"dimension_names", {"x", "y"} }, // required
            {"dimension_units", {"m", "ft"} }, // optional (if coord).
            {"extra_attributes",  // optional misc attributes.
                {
                    {"job status", "win"}, // can be anything
                    {"project code", "fail"}
                }
            },
        }
    },
    {"metadata",
        {
            {"compressor", {{"id", "blosc"}}},
            {"dtype", "<i2"},
            {"shape", {500, 500}},
            {"chunks", {100, 50}},
            {"dimension_separator", "/"},
        }
    }
};

TEST(VariableCollectionTest, add) {
    auto variable = mdio::Variable<>::Open(json_good, mdio::constants::kCreateClean);
    ASSERT_TRUE(variable.status().ok()) << variable.status();

    std::string name = "collectionVariable";

    mdio::VariableCollection vc;
    vc.add(name, variable.value());
}

TEST(VariableCollectionTest, get) {
    auto variable = mdio::Variable<>::Open(json_good, mdio::constants::kCreateClean);
    ASSERT_TRUE(variable.status().ok()) << variable.status();

    mdio::VariableCollection vc;

    std::string name = "collectionVariable";
    vc.add(name, variable.value());

    auto goodGet = vc.get(name);
    EXPECT_TRUE(goodGet.status().ok()) << goodGet.status();

    std::string badName = "DNE";

    auto doesNotExist = vc.get(badName);
    EXPECT_FALSE(doesNotExist.status().ok()) << doesNotExist.status();

    auto badCast = vc.get<float>(name);
    EXPECT_FALSE(badCast.status().ok()) << badCast.status();
    
}

TEST(VariableCollectionTest, containsKey) {
    auto variable = mdio::Variable<>::Open(json_good, mdio::constants::kCreateClean);
    ASSERT_TRUE(variable.status().ok()) << variable.status();

    mdio::VariableCollection vc;

    std::string name = "collectionVariable";
    std::string badName = "DNE";
    vc.add(name, variable.value());

    EXPECT_TRUE(vc.contains_key(name));
    EXPECT_FALSE(vc.contains_key(badName));
}

TEST(VariableCollectionTest, getKeys) {
    auto variable = mdio::Variable<>::Open(json_good, mdio::constants::kCreateClean);
    ASSERT_TRUE(variable.status().ok()) << variable.status();

    mdio::VariableCollection vc;

    EXPECT_EQ(vc.get_keys().size(), 0);

    std::string name = "collectionVariable";
    vc.add(name, variable.value());

    auto keys = vc.get_keys();
    ASSERT_EQ(vc.get_keys().size(), 1);
    EXPECT_EQ(keys[0], name);
}

} // namespace