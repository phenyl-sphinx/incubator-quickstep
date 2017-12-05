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

#ifndef QUICKSTEP_TRANSACTION_DOUBLE_SIDED_RANGE_PREDICATE_HPP_
#define QUICKSTEP_TRANSACTION_DOUBLE_SIDED_RANGE_PREDICATE_HPP_

#include "transaction/Predicate.hpp"
#include "transaction/RangePredicate.hpp"

namespace quickstep {
namespace transaction {

class DoubleSidedRangePredicate : public Predicate
{
private:

  const LessComparison* lessThanComp;
  const LessOrEqualComparison* lessThanEqComp;
  const GreaterComparison* greaterThanComp;
  const GreaterOrEqualComparison* greaterThanEqComp;

public:
  const RangePredicate leftBound;
  const RangePredicate rightBound;
  DoubleSidedRangePredicate(relation_id rel_id, attribute_id attr_id, RangePredicate leftBound, RangePredicate rightBound);
  ~DoubleSidedRangePredicate();
  bool intersect(const Predicate& predicate) const override;
};

}
}

#endif
