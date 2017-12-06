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


#include "transaction/Predicate.hpp"
#include "transaction/AnyPredicate.hpp"
#include "transaction/EqualityPredicate.hpp"
#include "transaction/RangePredicate.hpp"
#include "transaction/DoubleSidedRangePredicate.hpp"
#include "types/TypeID.hpp"
#include "types/TypedValue.hpp"
#include "types/IntType.hpp"
#include "types/LongType.hpp"
#include "types/FloatType.hpp"

#include "utility/Macros.hpp"

namespace quickstep {
namespace transaction {

Predicate::Predicate(relation_id rel_id, attribute_id attr_id):
rel_id(rel_id), attr_id(attr_id) {
}

bool Predicate::comparable(const Predicate* predicate) const{
  if((rel_id == predicate->rel_id) && (attr_id == predicate->attr_id))
    return true;
  else
    return false;
}

std::vector<std::shared_ptr<Predicate>> Predicate::combineConjuncts(std::vector<std::shared_ptr<Predicate>> a, std::vector<std::shared_ptr<Predicate>> b) {
  std::vector<std::shared_ptr<Predicate>> ret;
  for(auto a_itr=a.begin(); a_itr!=a.end(); a_itr++){
    auto a_element = *a_itr;

    for(auto b_itr=b.begin(); b_itr!=b.end(); b_itr++){
      auto b_element = *b_itr;
      if(a_element->intersect(*b_element)){
        if(a_element->type == Any){
          // a is ANY
          b.erase(b_itr);
          b_itr--;
        }
        else if((a_element->type == Range) || (a_element->type == DoubleSidedRange)){
          if(b_element->type == Equality) {
            b.erase(b_itr);
            b_itr--;
          }
          else if((a_element->type == Range) && (b_element->type == Range)){
            // Both are Range
            *a_itr = MergeRange(a_element, b_element);
            b.erase(b_itr);
            b_itr--;
          }
          else{
            // TODO: Merge other cases, not necessary
          }
        }
        else{
          // both are equality
        }
      }
    }
  }
  ret.reserve( a.size() + b.size() ); // preallocate memory
  ret.insert( ret.end(), a.begin(), a.end() );
  ret.insert( ret.end(), b.begin(), b.end() );
  return ret;
}

std::vector<std::shared_ptr<Predicate>> Predicate::breakdownHelper(serialization::Predicate& predicate){
  std::vector<std::shared_ptr<Predicate>> ret;

  switch (predicate.predicate_type()) {
    case 0:
      // ret.push_back( std::make_shared<AnyPredicate>()); TODO: return an any predicate on proper relation_id & attribute_id
      break;
    case 1:
      break;
    case 2: //Comparison predicate
    {
      serialization::Scalar right = predicate.GetExtension(serialization::ComparisonPredicate::right_operand);
      serialization::Scalar left = predicate.GetExtension(serialization::ComparisonPredicate::left_operand);
      serialization::Comparison comp = predicate.GetExtension(serialization::ComparisonPredicate::comparison);

      const serialization::TypedValue leftValue = left.GetExtension(serialization::ScalarLiteral::literal);
      const serialization::Type leftType = left.GetExtension(serialization::ScalarLiteral::literal_type);
      const serialization::TypedValue rightValue = right.GetExtension(serialization::ScalarLiteral::literal);
      const serialization::Type rightType = right.GetExtension(serialization::ScalarLiteral::literal_type);

      if(comp.comparison_id()==comp.EQUAL) {
        if(left.data_source()==left.ATTRIBUTE && right.data_source()==right.LITERAL) {

          TypedValue s;
          s=s.ReconstructFromProto(rightValue);
          const Type& t = TypeFactory::ReconstructFromProto(rightType);

          std::shared_ptr<Predicate> eqPredicate = std::make_shared<EqualityPredicate>(left.GetExtension(serialization::ScalarAttribute::relation_id),
            left.GetExtension(serialization::ScalarAttribute::attribute_id), t, s);
          ret.push_back(eqPredicate);
        }
        else if(left.data_source()==left.LITERAL && right.data_source()==right.ATTRIBUTE){

          TypedValue s;
          s=s.ReconstructFromProto(leftValue);
          const Type& t = TypeFactory::ReconstructFromProto(leftType);

          std::shared_ptr<Predicate> eqPredicate = std::make_shared<EqualityPredicate>(right.GetExtension(serialization::ScalarAttribute::relation_id),
            right.GetExtension(serialization::ScalarAttribute::attribute_id), t, s);
          ret.push_back(eqPredicate);
        }
        else if(left.data_source()==left.ATTRIBUTE && right.data_source()==right.ATTRIBUTE){

          std::shared_ptr<Predicate> rPredicate = std::make_shared<AnyPredicate>(right.GetExtension(serialization::ScalarAttribute::relation_id),
            right.GetExtension(serialization::ScalarAttribute::attribute_id));
          ret.push_back(rPredicate);

          std::shared_ptr<Predicate> lPredicate = std::make_shared<AnyPredicate>(left.GetExtension(serialization::ScalarAttribute::relation_id),
            left.GetExtension(serialization::ScalarAttribute::attribute_id));
          ret.push_back(lPredicate);
        }
        else {
          // TODO: Other cases
        }
      }
      else if((comp.comparison_id()==comp.LESS) ||
        (comp.comparison_id()==comp.LESS_OR_EQUAL) ||
        (comp.comparison_id()==comp.GREATER) ||
        (comp.comparison_id()==comp.GREATER_OR_EQUAL))
      {
        RangePredicate::RangeType rangeType = (comp.comparison_id()==comp.LESS) ? RangePredicate::SmallerThan :
          (comp.comparison_id()==comp.LESS_OR_EQUAL) ? RangePredicate::SmallerEqTo :
          (comp.comparison_id()==comp.GREATER) ? RangePredicate::LargerThan :
          RangePredicate::LargerEqTo;
        if(left.data_source()==left.ATTRIBUTE && right.data_source()==right.LITERAL) {

          TypedValue s;
          s=s.ReconstructFromProto(rightValue);
          const Type& t = TypeFactory::ReconstructFromProto(rightType);

          std::shared_ptr<Predicate> rgPredicate = std::make_shared<RangePredicate>(left.GetExtension(serialization::ScalarAttribute::relation_id),
            left.GetExtension(serialization::ScalarAttribute::attribute_id), t, s, rangeType);
          ret.push_back(rgPredicate);
        }
        else if(left.data_source()==left.LITERAL && right.data_source()==right.ATTRIBUTE){

          TypedValue s;
          s=s.ReconstructFromProto(leftValue);
          const Type& t = TypeFactory::ReconstructFromProto(leftType);

          std::shared_ptr<Predicate> eqPredicate = std::make_shared<RangePredicate>(right.GetExtension(serialization::ScalarAttribute::relation_id),
            right.GetExtension(serialization::ScalarAttribute::attribute_id), t, s, rangeType);
          ret.push_back(eqPredicate);
        }
        else if(left.data_source()==left.ATTRIBUTE && right.data_source()==right.ATTRIBUTE){

          std::shared_ptr<Predicate> rPredicate = std::make_shared<AnyPredicate>(right.GetExtension(serialization::ScalarAttribute::relation_id),
            right.GetExtension(serialization::ScalarAttribute::attribute_id));
          ret.push_back(rPredicate);

          std::shared_ptr<Predicate> lPredicate = std::make_shared<AnyPredicate>(left.GetExtension(serialization::ScalarAttribute::relation_id),
            left.GetExtension(serialization::ScalarAttribute::attribute_id));
          ret.push_back(lPredicate);
        }
      }
      break;
    }
    case 3: // Negation
    {
      // TODO:
    }
    case 4: //Conjunction
    {
      serialization::Predicate left = predicate.GetExtension(serialization::PredicateWithList::operands,0);
      serialization::Predicate right = predicate.GetExtension(serialization::PredicateWithList::operands,1);

      std::vector<std::shared_ptr<Predicate>> left_list = breakdownHelper(left);
      std::vector<std::shared_ptr<Predicate>> right_list = breakdownHelper(right);

      ret = combineConjuncts(left_list, right_list);
    }
    case 5: // Disjunction
    {

      for(size_t i = 0; i < predicate.ExtensionSize(serialization::PredicateWithList::operands); i++){

        serialization::Predicate subPred = predicate.GetExtension(serialization::PredicateWithList::operands,0);

        std::vector<std::shared_ptr<Predicate>> sub_list = breakdownHelper(subPred);

        ret.insert( ret.end(), sub_list.begin(), sub_list.end() );
      }

    }
    default:
      break;
  }

  return ret;
}

std::vector<std::shared_ptr<Predicate>> Predicate::breakdown(serialization::Predicate& predicate){
  // LOG_WARNING("Breakdown size");
  // LOG_WARNING(breakdownHelper(predicate).size());
  return breakdownHelper(predicate);
}

std::shared_ptr<Predicate> Predicate::MergeRange(std::shared_ptr<Predicate> raw_a, std::shared_ptr<Predicate> raw_b){
  // if((raw_a->type != Range) || (raw_b->type != Range))
  //   return std::make_shared<Predicate>(nullptr);

  auto a = std::dynamic_pointer_cast<RangePredicate>(raw_a);
  auto b = std::dynamic_pointer_cast<RangePredicate>(raw_b);


  auto eqComp = &quickstep::EqualComparison::Instance();
  auto lessThanComp = &quickstep::LessComparison::Instance();
  auto greaterThanComp = &quickstep::GreaterComparison::Instance();

  if (
    ((a->rangeType == RangePredicate::LargerThan) || (a->rangeType == RangePredicate::LargerEqTo)) &&
    ((b->rangeType == RangePredicate::LargerThan) || (b->rangeType == RangePredicate::LargerEqTo))
  ) {
    if(eqComp->compareTypedValuesChecked(b->targetValue, b->targetType, a->targetValue, a->targetType)){
      // the two predicate value are the same
      if(b->rangeType == RangePredicate::LargerEqTo){
        return std::make_shared<RangePredicate>(a->rel_id, a->attr_id, a->targetType, a->targetValue, a->rangeType);
      }
      else if(b->rangeType == RangePredicate::LargerEqTo){
        return std::make_shared<RangePredicate>(b->rel_id, b->attr_id, b->targetType, b->targetValue, b->rangeType);
      }
      else{
        return std::make_shared<RangePredicate>(a->rel_id, a->attr_id, a->targetType, a->targetValue, a->rangeType);
      }
    }
    else {
      // use the smaller one
      if(lessThanComp->compareTypedValuesChecked(a->targetValue, a->targetType, b->targetValue, b->targetType)){
        return std::make_shared<RangePredicate>(a->rel_id, a->attr_id, a->targetType, a->targetValue, a->rangeType);
      }
      else{
        return std::make_shared<RangePredicate>(b->rel_id, b->attr_id, b->targetType, b->targetValue, b->rangeType);
      }
    }
  }
  else if (
    ((a->rangeType == RangePredicate::SmallerThan) || (a->rangeType == RangePredicate::SmallerEqTo)) &&
    ((b->rangeType == RangePredicate::SmallerThan) || (b->rangeType == RangePredicate::SmallerThan))
  ){
    if(eqComp->compareTypedValuesChecked(b->targetValue, b->targetType, a->targetValue, a->targetType)){
      // the two predicate value are the same
      if(b->rangeType == RangePredicate::SmallerEqTo){
        return std::make_shared<RangePredicate>(a->rel_id, a->attr_id, a->targetType, a->targetValue, a->rangeType);
      }
      else if(b->rangeType == RangePredicate::SmallerEqTo){
        return std::make_shared<RangePredicate>(b->rel_id, b->attr_id, b->targetType, b->targetValue, b->rangeType);
      }
      else{
        return std::make_shared<RangePredicate>(a->rel_id, a->attr_id, a->targetType, a->targetValue, a->rangeType);
      }
    }
    else {
      // use the larger one
      if(greaterThanComp->compareTypedValuesChecked(a->targetValue, a->targetType, b->targetValue, b->targetType)){
        return std::make_shared<RangePredicate>(a->rel_id, a->attr_id, a->targetType, a->targetValue, a->rangeType);
      }
      else{
        return std::make_shared<RangePredicate>(b->rel_id, b->attr_id, b->targetType, b->targetValue, b->rangeType);
      }
    }
  }
  else{
    // The two comparison are facing different direction, can be merged into a double-sided-range
    if((a->rangeType == RangePredicate::SmallerThan) || (a->rangeType == RangePredicate::SmallerEqTo)) {
      // the current one is the right bound
      return std::make_shared<DoubleSidedRangePredicate>(a->rel_id, a->attr_id, *b, *a);
    }
    else {
      // the current one is the left bound
      return std::make_shared<DoubleSidedRangePredicate>(a->rel_id, a->attr_id, *a, *b);
    }
  }
}

}
}
