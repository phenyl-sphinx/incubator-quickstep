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

#include "transaction/DoubleSidedRangePredicate.hpp"

namespace quickstep {
namespace transaction {


DoubleSidedRangePredicate::DoubleSidedRangePredicate(relation_id rel_id, attribute_id attr_id, RangePredicate leftBound, RangePredicate rightBound):
Predicate(rel_id, attr_id), leftBound(leftBound), rightBound(rightBound)
{
  type = DoubleSidedRange;

  lessThanComp = &quickstep::LessComparison::Instance();
  lessThanEqComp = &quickstep::LessOrEqualComparison::Instance();
  greaterThanComp = &quickstep::GreaterComparison::Instance();
  greaterThanEqComp = &quickstep::GreaterOrEqualComparison::Instance();

  // TODO: Verify the validity of the left and right bound
}

DoubleSidedRangePredicate::~DoubleSidedRangePredicate(){
}

bool DoubleSidedRangePredicate::intersect(const Predicate& predicate) const{
  if(predicate.rel_id != rel_id || predicate.attr_id != attr_id)
    return false;

  switch(predicate.type){
    case Any: {
      return true;
    }
    case Equality: {
      return leftBound.intersect(predicate) && rightBound.intersect(predicate);
    }
    case Range: {
      return leftBound.intersect(predicate) && rightBound.intersect(predicate);
    }
    case DoubleSidedRange: {
      DoubleSidedRangePredicate *drgPredicate = (DoubleSidedRangePredicate *)(&predicate);
      return leftBound.intersect(drgPredicate->rightBound) || rightBound.intersect(drgPredicate->leftBound);
    }
    default: {
      return false;
    }
  }

  return true; // TODO: Implement this
}


}
}
