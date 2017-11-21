/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 **/

#include "transaction/CycleDetector.hpp"

#include <cstdint>
#include <memory>
#include <stack>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "transaction/Predicate.hpp"
#include "transaction/AnyPredicate.hpp"
#include "types/TypeID.hpp"
#include "types/TypedValue.hpp"
#include "types/IntType.hpp"
#include "types/LongType.hpp"
#include "types/FloatType.hpp"

namespace quickstep {
namespace transaction {

AnyPredicate::AnyPredicate(relation_id rel_id, attribute_id attr_id):
Predicate(rel_id, attr_id)
{
}

AnyPredicate::~AnyPredicate(){
}

bool AnyPredicate::intersect(const Predicate& predicate) const{
  if(predicate.rel_id != rel_id || predicate.attr_id != attr_id)
    return false;

  return true;
}

}  // namespace transaction
}  // namespace quickstep
