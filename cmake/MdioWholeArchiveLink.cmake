# Copyright 2026 TGS
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Link wrapper used by MdioMonolithicShared.cmake to build libmdio_monolith.so.
#
# It is invoked as:
#   cmake -P MdioWholeArchiveLink.cmake -- <compiler> <flags...> -o <out> \
#         <objects...> <link-libraries...>
#
# and rewrites the link command so the .so contains the *entire* static
# dependency closure:
#   * every static archive (*.a) is de-duplicated, keeping the first occurrence.
#     CMake lists some archives twice for circular-dependency resolution with
#     traditional linkers (GNU ld / gold); under --whole-archive that would cause
#     "multiple definition" errors. (CMake's own CMP0156/CMP0179 de-duplication
#     only kicks in for symbol-recording linkers such as LLD, so we cannot rely
#     on it with the default toolchain.)
#   * all archives are bracketed with --whole-archive/--no-whole-archive so
#     objects the .so's own code never references are still included. A consumer
#     that instantiates a tensorstore/Abseil/riegeli template from a header then
#     resolves the out-of-line symbol from the .so instead of failing to link.
#   * shared/system libraries (-l...) are placed after --no-whole-archive so the
#     compiler-implicit trailing libraries (e.g. libgcc) are never whole-archived.
#
# This wrapper is generic (it does not encode anything tensorstore-specific), so
# it needs no maintenance as the dependency graph evolves.

# Collect the arguments that follow the "--" sentinel.
set(_args "")
set(_seen_sep FALSE)
math(EXPR _last "${CMAKE_ARGC} - 1")
foreach(_i RANGE 0 ${_last})
  set(_a "${CMAKE_ARGV${_i}}")
  if(_seen_sep)
    list(APPEND _args "${_a}")
  elseif(_a STREQUAL "--")
    set(_seen_sep TRUE)
  endif()
endforeach()

if(NOT _seen_sep)
  message(FATAL_ERROR "MdioWholeArchiveLink: missing '--' argument separator")
endif()

set(_head "")       # compiler, flags, -o <out>, objects, misc linker flags
set(_archives "")   # unique static archives, in first-seen order
set(_libs "")       # -l... shared/system libraries (kept after the archives)
set(_seen_archives "")

foreach(_a IN LISTS _args)
  if(_a STREQUAL "-Wl,--push-state,--whole-archive" OR _a STREQUAL "-Wl,--pop-state")
    # Drop tensorstore's per-archive whole-archive wrappers; the global bracket
    # below subsumes them and the stray push/pop would otherwise be unbalanced
    # once archives are reordered.
    continue()
  elseif(_a MATCHES "\\.a$")
    list(FIND _seen_archives "${_a}" _found)
    if(_found EQUAL -1)
      list(APPEND _seen_archives "${_a}")
      list(APPEND _archives "${_a}")
    endif()
  elseif(_a MATCHES "^-l")
    list(APPEND _libs "${_a}")
  else()
    list(APPEND _head "${_a}")
  endif()
endforeach()

set(_cmd
  ${_head}
  -Wl,--whole-archive
  ${_archives}
  -Wl,--no-whole-archive
  ${_libs}
)

execute_process(COMMAND ${_cmd} RESULT_VARIABLE _rc)
if(NOT _rc EQUAL 0)
  message(FATAL_ERROR "MdioWholeArchiveLink: link command failed (${_rc})")
endif()
