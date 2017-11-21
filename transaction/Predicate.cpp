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

#include <memory>
#include <unordered_set>
#include <vector>

namespace quickstep {
namespace transaction {

std::vector<shared_ptr<Predicate>> Predicate::breakdownHelper(serialization::Predicate& predicate){
  std::vector<shared_ptr<Predicate>> ret;

  switch (predicate.predicate_type()) {
    case 0:
      // ret.push_back( make_shared<AnyPredicate>()); TODO: Implement ANY predicate
      break;
    case 1:
      break;
    case 2: //Comparison predicate
      serialization::Scalar right = predicate.GetExtension(serialization::ComparisonPredicate::right_operand);
      serialization::Scalar left = predicate.GetExtension(serialization::ComparisonPredicate::left_operand);
      serialization::Comparison comp = predicate.GetExtension(serialization::ComparisonPredicate::comparison);

      if(comp.comparison_id()==comp.EQUAL) {
        if(left.data_source()==left.ATTRIBUTE && right.data_source()==right.LITERAL) {
          const serialization::TypedValue rightValue = right.GetExtension(serialization::ScalarLiteral::literal);
          const serialization::Type rightType = right.GetExtension(serialization::ScalarLiteral::literal_type);
          TypedValue s;
          s=s.ReconstructFromProto(rightValue);
          const Type* t = &TypeFactory::ReconstructFromProto(rightType);

          shared_ptr<Predicate> eqPredicate = make_shared<EqualityPredicate>(t, &s);
          ret.push_back(eqPredicate);
        }
        else if(left.data_source()==left.LITERAL && right.data_source()==right.ATTRIBUTE){
          const serialization::TypedValue leftValue = left.GetExtension(serialization::ScalarLiteral::literal);
          const serialization::Type leftType = left.GetExtension(serialization::ScalarLiteral::literal_type);
          TypedValue s;
          s=s.ReconstructFromProto(leftValue);
          const Type* t = &TypeFactory::ReconstructFromProto(leftType);

          shared_ptr<Predicate> eqPredicate = make_shared<EqualityPredicate>(t, &s);
          ret.push_back(eqPredicate);
        }
      }
      //TODO rest of comparisons
      break;
    //case 4: //Conjunction

     // break;
      //TODO rest of predicate types
    //default:

    //  break;
  }

  return ret;
}

std::vector<shared_ptr<Predicate>> Predicate::breakdown(serialization::Predicate& predicate){
  return breakdownHelper(predicate);
}

}
}
