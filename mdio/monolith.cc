// Copyright 2026 TGS
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Translation-unit anchor for the optional monolithic shared library
// (MDIO_BUILD_MONOLITHIC_SHARED). The heavy tensorstore/Abseil objects -- and
// the driver self-registration initializers -- are force-included via
// WHOLE_ARCHIVE in CMake; this file just gives the shared object a concrete,
// exported symbol and pulls the public header so the include graph is built.
//
// Why this library exists: a process that loads more than one plugin which
// each statically embed tensorstore/Abseil ends up with multiple copies of
// Abseil's global state (e.g. the Cord registry), which aborts at runtime
// ("ODR violation in Cord"). Linking every such plugin against this single
// shared library instead means the dynamic linker maps tensorstore/Abseil
// exactly once, so those globals are singletons again.

#include <mdio/mdio.h>

extern "C" const char* mdio_monolith_version() { return "1.0.0"; }
