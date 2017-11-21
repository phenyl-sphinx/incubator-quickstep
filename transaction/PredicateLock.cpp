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

#include "transaction/PredicateLock.hpp"
#include "transaction/Predicate.hpp"
#include "types/TypeID.hpp"
#include "types/TypedValue.hpp"
#include "types/IntType.hpp"
#include "types/LongType.hpp"
#include "types/FloatType.hpp"
#include "types/operations/comparisons/EqualComparison.hpp"
#include "types/operations/comparisons/LessComparison.hpp"
#include "types/operations/comparisons/LessOrEqualComparison.hpp"
#include "types/operations/comparisons/GreaterComparison.hpp"
#include "types/operations/comparisons/GreaterOrEqualComparison.hpp"

namespace quickstep {
namespace transaction {
  
  PredicateLock::PredicateLock(){}

bool PredicateLock::intersect(const PredicateLock& lock) const{
  for(transaction::Predicate *thisPredicate: read_predicates){
    for(transaction::Predicate *thatPredicate: lock.read_predicates){
      if(thisPredicate->intersect(*thatPredicate))
      {
        return true;
      }
    }
    for(transaction::Predicate *thatPredicate: lock.write_predicates){
      if(thisPredicate->intersect(*thatPredicate))
      {
        return true;
      }
    }
  }
  return false;
}
  
bool PredicateLock::addPredicateWrite(transaction::Predicate* predicate) {
  write_predicates.push_back(predicate);
  return true;
}
  
bool PredicateLock::addPredicateRead(transaction::Predicate* predicate) {
  read_predicates.push_back(predicate);
  return true;
}

}  // namespace transaction
}  // namespace quickstep
