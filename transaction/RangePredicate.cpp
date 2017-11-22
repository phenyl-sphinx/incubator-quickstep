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
#include "transaction/EqualityPredicate.hpp"
#include "transaction/RangePredicate.hpp"
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

RangePredicate::RangePredicate(relation_id rel_id, attribute_id attr_id,
  const Type& targetType,
  const TypedValue targetValue,
  RangeType rangeType):
Predicate(rel_id, attr_id), rangeType(rangeType), targetType(targetType), targetValue(targetValue)
{
  type = Range;

  lessThanComp = &quickstep::LessComparison::Instance();
  lessThanEqComp = &quickstep::LessOrEqualComparison::Instance();
  greaterThanComp = &quickstep::GreaterComparison::Instance();
  greaterThanEqComp = &quickstep::GreaterOrEqualComparison::Instance();
}

RangePredicate::~RangePredicate(){
}

bool RangePredicate::intersect(const Predicate& predicate) const{
  if(predicate.rel_id != rel_id || predicate.attr_id != attr_id)
    return false;

  switch(predicate.type){
    case Any: {
      return true;
    }
    case Equality: {
      const EqualityPredicate* eqPredicate = (EqualityPredicate *)(&predicate);

      switch (rangeType) {
        case LargerThan:
          return greaterThanComp->compareTypedValuesChecked(eqPredicate->targetValue, eqPredicate->targetType, targetValue, targetType);
        case LargerEqTo:
          return greaterThanEqComp->compareTypedValuesChecked(eqPredicate->targetValue, eqPredicate->targetType, targetValue, targetType);
        case SmallerThan:
          return lessThanComp->compareTypedValuesChecked(eqPredicate->targetValue, eqPredicate->targetType, targetValue, targetType);
        case SmallerEqTo:
          return lessThanEqComp->compareTypedValuesChecked(eqPredicate->targetValue, eqPredicate->targetType, targetValue, targetType);
        default:
          return false;
      }
    }
    case Range: {
      const RangePredicate* rgPredicate = (RangePredicate *)(&predicate);

      switch (rangeType) {
        case LargerThan:{
          switch(rgPredicate->rangeType){
            case LargerThan:
              return true;
            case LargerEqTo:
              return true;
            case SmallerThan:
              return greaterThanComp->compareTypedValuesChecked(rgPredicate->targetValue, rgPredicate->targetType, targetValue, targetType);
            case SmallerEqTo:
              return greaterThanComp->compareTypedValuesChecked(rgPredicate->targetValue, rgPredicate->targetType, targetValue, targetType);
            default:
              return false;
          }
        }
        case LargerEqTo:{
          switch(rgPredicate->rangeType){
            case LargerThan:
              return true;
            case LargerEqTo:
              return true;
            case SmallerThan:
              return greaterThanComp->compareTypedValuesChecked(rgPredicate->targetValue, rgPredicate->targetType, targetValue, targetType);
            case SmallerEqTo:
              return greaterThanEqComp->compareTypedValuesChecked(rgPredicate->targetValue, rgPredicate->targetType, targetValue, targetType);
            default:
              return false;
          }
        }
        case SmallerThan:{
          switch(rgPredicate->rangeType){
            case LargerThan:
              return lessThanComp->compareTypedValuesChecked(rgPredicate->targetValue, rgPredicate->targetType, targetValue, targetType);
            case LargerEqTo:
              return lessThanComp->compareTypedValuesChecked(rgPredicate->targetValue, rgPredicate->targetType, targetValue, targetType);
            case SmallerThan:
              return true;
            case SmallerEqTo:
              return true;
            default:
              return false;
          }
        }
        case SmallerEqTo:{
          switch(rgPredicate->rangeType){
            case LargerThan:
              return lessThanComp->compareTypedValuesChecked(rgPredicate->targetValue, rgPredicate->targetType, targetValue, targetType);
            case LargerEqTo:
              return lessThanEqComp->compareTypedValuesChecked(rgPredicate->targetValue, rgPredicate->targetType, targetValue, targetType);
            case SmallerThan:
              return true;
            case SmallerEqTo:
              return true;
            default:
              return false;
          }
        }
        default:
          return false;
      }
    }
    default:
      return false;
  }
}

}  // namespace transaction
}  // namespace quickstep
