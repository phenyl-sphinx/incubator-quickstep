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

#include "catalog/Catalog.pb.h"
#include "catalog/CatalogConfig.h"
#include "catalog/CatalogRelationSchema.hpp"
#include "catalog/CatalogRelationStatistics.hpp"
#include "catalog/CatalogTypedefs.hpp"
#include "catalog/IndexScheme.hpp"

#ifdef QUICKSTEP_HAVE_LIBNUMA
#include "catalog/NUMAPlacementScheme.hpp"
#endif  // QUICKSTEP_HAVE_LIBNUMA

#include "catalog/PartitionScheme.hpp"
#include "storage/StorageBlockInfo.hpp"
#include "storage/StorageBlockLayout.hpp"
#include "storage/StorageConstants.hpp"
#include "threading/Mutex.hpp"
#include "threading/SharedMutex.hpp"
#include "threading/SpinSharedMutex.hpp"
#include "utility/Macros.hpp"

#include "transaction/Predicate.hpp"
#include "transaction/AnyPredicate.hpp"
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

Predicate::Predicate(relation_id rel_id, attribute_id attr_id):
rel_id(rel_id), attr_id(attr_id) {
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
        else{
          // TODO: Any Predicate on both attribute
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
      // TODO:
    }
    case 5: // Disjunction
    {
      // TODO:
    }
    default:
      break;
  }

  return ret;
}

std::vector<std::shared_ptr<Predicate>> Predicate::breakdown(serialization::Predicate& predicate){
  return breakdownHelper(predicate);
}

}
}
