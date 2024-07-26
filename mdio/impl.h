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

#ifndef MDIO_IMPL_H_
#define MDIO_IMPL_H_
#include "tensorstore/tensorstore.h"

/**
 * MDIO makes extensive use of the Tensorstore library.
 * Ideally the user should never know that Tensorstore is being used under the
 * hood. For that reason we have updated the namespaces that should be
 * user-facing to reflect the API conventions. It is not indended to be an
 * exhaustive namespacing of the internal usages, although the API should still
 * use these namespaces wherever possible.
 */

#define MDIO_ASSIGN_OR_RETURN(...) TENSORSTORE_ASSIGN_OR_RETURN(__VA_ARGS__)

namespace mdio {

// External bleeds
using DimensionIdentifier = tensorstore::DimensionIdentifier;
using Index = tensorstore::Index;
using DimensionIndex = tensorstore::DimensionIndex;
using ReadWriteMode = tensorstore::ReadWriteMode;
using ArrayOriginKind = tensorstore::ArrayOriginKind;
template <typename T>
using Future = tensorstore::Future<T>;
using WriteFutures = tensorstore::WriteFutures;
template <long int Rank>
using IndexDomainView = tensorstore::IndexDomainView<Rank>;
using DataType = tensorstore::DataType;
template <typename T>
using Result = tensorstore::Result<T>;
using Spec = tensorstore::Spec;
using MustAllocateConstraint = tensorstore::MustAllocateConstraint;
template <typename T, DimensionIndex R, ArrayOriginKind OriginKind>
using SharedArray =
    tensorstore::SharedArray<T, R, OriginKind, tensorstore::container>;
using TransactionalOpenOptions = tensorstore::TransactionalOpenOptions;
using IncludeDefaults = tensorstore::IncludeDefaults;
using Context = tensorstore::Context;

// Type bleeds
namespace dtypes {
using uint8_t = tensorstore::dtypes::uint8_t;
using uint16_t = tensorstore::dtypes::uint16_t;
using uint32_t = tensorstore::dtypes::uint32_t;
using uint64_t = tensorstore::dtypes::uint64_t;
using int8_t = tensorstore::dtypes::int8_t;
using int16_t = tensorstore::dtypes::int16_t;
using int32_t = tensorstore::dtypes::int32_t;
using int64_t = tensorstore::dtypes::int64_t;
using float_16_t =
    tensorstore::dtypes::float16_t;  // We seem to need to format float16 like
                                     // this for clean Clang compiling
using float32_t = tensorstore::dtypes::float32_t;
using float64_t = tensorstore::dtypes::float64_t;
using complex64_t = tensorstore::dtypes::complex64_t;
using complex128_t = tensorstore::dtypes::complex128_t;
using byte_t = tensorstore::dtypes::byte_t;
using bool_t = tensorstore::dtypes::bool_t;
}  // namespace dtypes

// Special constants bleeds
constexpr DimensionIndex dynamic_rank = tensorstore::dynamic_rank;
constexpr ArrayOriginKind zero_origin = tensorstore::zero_origin;
constexpr ArrayOriginKind offset_origin = tensorstore::offset_origin;

namespace ContiguousLayoutOrder {
// These are a 1 to 1 mapping of the Tensorstore enums to an MDIO namespace.
constexpr tensorstore::ContiguousLayoutOrder c =
    tensorstore::ContiguousLayoutOrder::c;
constexpr tensorstore::ContiguousLayoutOrder right =
    tensorstore::ContiguousLayoutOrder::right;
constexpr tensorstore::ContiguousLayoutOrder row_major =
    tensorstore::ContiguousLayoutOrder::row_major;
constexpr tensorstore::ContiguousLayoutOrder left =
    tensorstore::ContiguousLayoutOrder::left;
constexpr tensorstore::ContiguousLayoutOrder fortran =
    tensorstore::ContiguousLayoutOrder::fortran;
constexpr tensorstore::ContiguousLayoutOrder column_major =
    tensorstore::ContiguousLayoutOrder::column_major;
}  // namespace ContiguousLayoutOrder

/**
 * @brief A collection of frequently used constants when working with MDIO.
 * Currently it contains the following frequently used constants:
 * - OpenMode: Common operators for opening or creating a file.
 * - dtypes: Supported dtypes specifically called out by the MDIO V1.0
 * specification. These dtypes are intended for Variable type-checking and not
 * type-casting.
 */
namespace constants {
// Common open mode operators

/// Open a pre-existing file.
constexpr auto kOpen = tensorstore::OpenMode::open;
/// Create a new file and delete any existing file.
constexpr auto kCreateClean =
    (tensorstore::OpenMode::create | tensorstore::OpenMode::delete_existing);
/// Create a new file or error if it already exists.
constexpr auto kCreate = tensorstore::OpenMode::create;

// Supported dtypes
constexpr auto kBool = tensorstore::dtype_v<mdio::dtypes::bool_t>;
constexpr auto kInt8 = tensorstore::dtype_v<mdio::dtypes::int8_t>;
constexpr auto kInt16 = tensorstore::dtype_v<mdio::dtypes::int16_t>;
constexpr auto kInt32 = tensorstore::dtype_v<mdio::dtypes::int32_t>;
constexpr auto kInt64 = tensorstore::dtype_v<mdio::dtypes::int64_t>;
constexpr auto kUint8 = tensorstore::dtype_v<mdio::dtypes::uint8_t>;
constexpr auto kUint16 = tensorstore::dtype_v<mdio::dtypes::uint16_t>;
constexpr auto kUint32 = tensorstore::dtype_v<mdio::dtypes::uint32_t>;
constexpr auto kUint64 = tensorstore::dtype_v<mdio::dtypes::uint64_t>;
constexpr auto kFloat16 = tensorstore::dtype_v<mdio::dtypes::float_16_t>;
constexpr auto kFloat32 = tensorstore::dtype_v<mdio::dtypes::float32_t>;
constexpr auto kFloat64 = tensorstore::dtype_v<mdio::dtypes::float64_t>;
constexpr auto kComplex64 = tensorstore::dtype_v<mdio::dtypes::complex64_t>;
constexpr auto kComplex128 = tensorstore::dtype_v<mdio::dtypes::complex128_t>;
constexpr auto kByte = tensorstore::dtype_v<mdio::dtypes::byte_t>;
}  // namespace constants

}  // namespace mdio

#endif  // MDIO_IMPL_H_