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

#ifndef QUICKSTEP_TRANSACTION_PREDICATE_LOCK_HPP_
#define QUICKSTEP_TRANSACTION_PREDICATE_LOCK_HPP_

#include <memory>
#include <unordered_set>
#include <vector>
#include "transaction/PredicateLock.hpp"
#include "transaction/Predicate.hpp"
#include "types/Type.hpp"
#include "types/TypeID.hpp"
#include "types/TypedValue.hpp"
#include "types/operations/comparisons/Comparison.hpp"
#include "types/operations/comparisons/EqualComparison.hpp"
#include "types/operations/comparisons/LessComparison.hpp"
#include "types/operations/comparisons/LessOrEqualComparison.hpp"
#include "types/operations/comparisons/GreaterComparison.hpp"
#include "types/operations/comparisons/GreaterOrEqualComparison.hpp"

namespace quickstep {
namespace transaction {

class PredicateLock
{
private:
  std::vector<std::shared_ptr<Predicate>> read_predicates;
  std::vector<std::shared_ptr<Predicate>> write_predicates;

public:
  enum IsolationLevel
  {
    DirtyWrite,
    DirtyRead,
    FullySerializable,
  };
  IsolationLevel iso_level;
  PredicateLock();
  PredicateLock(IsolationLevel iso_level);
  bool intersect(const PredicateLock& lock) const;
  bool addPredicateWrite(std::shared_ptr<Predicate> predicate) ;
  bool addPredicateRead(std::shared_ptr<Predicate> predicate) ;
  bool coversAttribute(int relation, int attribute) ; 
};

}
}

#endif
