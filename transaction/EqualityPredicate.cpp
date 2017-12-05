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

#include "transaction/EqualityPredicate.hpp"
#include "transaction/AnyPredicate.hpp"
#include "transaction/RangePredicate.hpp"

#include "utility/Macros.hpp"

namespace quickstep {
namespace transaction {

EqualityPredicate::EqualityPredicate(relation_id rel_id, attribute_id attr_id, const Type& _targetType, const TypedValue _targetValue):
Predicate(rel_id, attr_id), targetType(_targetType), targetValue(_targetValue)
{
  type = Equality;

  eqComp = &quickstep::EqualComparison::Instance();
}

EqualityPredicate::~EqualityPredicate(){
}

bool EqualityPredicate::intersect(const Predicate& predicate) const{
  if(predicate.rel_id != rel_id || predicate.attr_id != attr_id)
    return false;

  switch(predicate.type){
    case Any: {
      return true;
    }
    case Equality: {
      const EqualityPredicate* eqPredicate = (EqualityPredicate *)&predicate;
      return eqComp->compareTypedValuesChecked(targetValue, targetType, eqPredicate->targetValue, eqPredicate->targetType);
    }
    case Range: {
      const RangePredicate* rgPredicate = (RangePredicate *)&predicate;
      return rgPredicate->intersect(*this);
    }
    default: {
      return false;
    }
  }
}

}  // namespace transaction
}  // namespace quickstep
