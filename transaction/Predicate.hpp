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

#ifndef QUICKSTEP_TRANSACTION_PREDICATE_HPP_
#define QUICKSTEP_TRANSACTION_PREDICATE_HPP_

#include <memory>
#include <unordered_set>
#include <vector>

#include "transaction/Predicate.hpp"

#include "catalog/CatalogDatabase.hpp"
#include "catalog/CatalogRelation.hpp"
#include "catalog/PartitionScheme.hpp"
#include "query_execution/QueryExecutionMessages.pb.h"
#include "query_execution/QueryExecutionState.hpp"
#include "query_execution/QueryExecutionTypedefs.hpp"
#include "query_execution/QueryManagerBase.hpp"

namespace quickstep {
namespace transaction {

class Predicate
{
private:


  static std::vector<std::shared_ptr<Predicate>> breakdownHelper(serialization::Predicate& predicate);

public:
  enum PredicateType
  {
    Any,
    Equality,
    Range,
    DoubleSidedRange
  };
  relation_id rel_id;
  attribute_id attr_id;
  PredicateType type;


  static std::vector<std::shared_ptr<Predicate>> breakdown(serialization::Predicate& predicate);
  Predicate(relation_id rel_id, attribute_id attr_id);
  virtual ~Predicate(){};
  virtual bool intersect(const Predicate& predicate) const = 0;
};

}
}

#endif
