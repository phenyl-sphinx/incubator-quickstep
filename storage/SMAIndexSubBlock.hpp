/**
 *   Copyright 2016 Pivotal Software, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 **/

#ifndef QUICKSTEP_STORAGE_SMA_INDEX_SUB_BLOCK_HPP_
#define QUICKSTEP_STORAGE_SMA_INDEX_SUB_BLOCK_HPP_

#include <cstddef>
#include <exception>
#include <iostream>
#include <string>
#include <vector>

#include "catalog/CatalogAttribute.hpp"
#include "expressions/aggregation/AggregationHandleSum.hpp"
#include "expressions/predicate/PredicateCost.hpp"
#include "storage/IndexSubBlock.hpp"
#include "storage/StorageConstants.hpp"
#include "storage/SubBlockTypeRegistryMacros.hpp"
#include "types/TypedValue.hpp"
#include "types/operations/binary_operations/BinaryOperation.hpp"
#include "types/operations/comparisons/Comparison.hpp"
#include "types/operations/comparisons/ComparisonID.hpp"
#include "types/operations/comparisons/ComparisonFactory.hpp"
#include "utility/BitVector.hpp"
#include "utility/Macros.hpp"
#include "utility/PtrVector.hpp"

#include "glog/logging.h"

namespace quickstep {

class SMAIndexSubBlock;

QUICKSTEP_DECLARE_SUB_BLOCK_TYPE_REGISTERED(SMAIndexSubBlock);

namespace sma_internal {

//// Predicate answering components.

/**
 * @brief Describes how much of the relation will be selected by a predicate.
 * @details kAll, kNone indicate that the SMA has determined that all, or none 
 *          of the tuples will be selected. kSome means that some tuples may be
 *          selected, but a scan must be performed. kUnknown indicates that the
 *          SMA tried to answer the predicate but did not have enough information.
 *          kUnsolved indicates that the predicate has been created but not 
 *          analyzed by the SMA.  
 */
enum class Selectivity {
  kAll,
  kSome,
  kNone,
  kUnknown,
  kUnsolved
};

/**
 * @brief Helper method. Uses the stored values from the SMA Index to determine
 *        selectivity of a predicate.
 * 
 * @param literal 
 * @param min 
 * @param max 
 * @param equals_comparator 
 * @param less_comparator 
 * @return Selectivity of this predicate.
 */
Selectivity getSelectivity(const TypedValue &literal,
                           const ComparisonID comparison,
                           const TypedValue &min,
                           const TypedValue &max,
                           const UncheckedComparator *equals_comparator,
                           const UncheckedComparator *less_comparator);

/**
 * @brief A simple holding struct for a comparison predicate. Selectivity enum
 *        indicates if the SMA has been used to solve the predicate and if so,
 *        what is the selectivity over the block.
 */
struct SMAPredicate {
  /**
   * @brief Extracts a comparison predicate into an SMAPredicate.
   * 
   * @param predicate A comparison of the form {attribute} {comparison} {literal}
   *                  or {literal} {comparison} {attribute}.
   * @return An SMAPredicate pointer which the caller must manage.
   */
  static SMAPredicate* ExtractSMAPredicate(const ComparisonPredicate& predicate);

  const attribute_id attribute_;
  const ComparisonID comparison_;
  const TypedValue literal_;
  Selectivity selectivity_;

 private:
  SMAPredicate(const attribute_id attribute,
               const ComparisonID comparisonid,
               const TypedValue literal) 
      : attribute_(attribute),
        comparison_(comparisonid),
        literal_(literal),
        selectivity_(Selectivity::kUnsolved) {};
};

//// Components of the index.

// A 64-bit header.
struct SMAHeader {
  std::uint32_t count_;
  union {
    bool consistent_;
    std::uint32_t buffer_;
  };
};

// Reference to an attribute value in a tuple.
struct EntryReference {
  tuple_id tuple_;
  bool valid_;
  TypedValue value_;
};

// Index entry for an attribute.
struct SMAEntry {
  attribute_id attribute_;
  TypeID type_;
  EntryReference min_entry_;
  EntryReference max_entry_;
  TypedValue sum_;
};

}  // namespace sma_internal


/**
 * @brief Small Materialized Aggregate SubBlock.
 * @details Keeps account of several types of aggregate functions per Block.
 *          Currently supports min, max, sum, and count.
 */
class SMAIndexSubBlock : public IndexSubBlock {
 public:
  SMAIndexSubBlock(const TupleStorageSubBlock &tuple_store,
                   const IndexSubBlockDescription &description,
                   const bool new_block,
                   void *sub_block_memory,
                   const std::size_t sub_block_memory_size);

  /**
   * @brief Frees data associated with variable length attributes.
   */
  ~SMAIndexSubBlock();

  /**
   * @brief Determine whether an IndexSubBlockDescription is valid for this
   *        type of IndexSubBlock.
   *
   * @param relation The relation an index described by description would
   *        belong to.
   * @param description A description of the parameters for this type of
   *        IndexSubBlock, which will be checked for validity.
   * @return Whether description is well-formed and valid for this type of
   *         IndexSubBlock belonging to relation (i.e. whether an IndexSubBlock
   *         of this type, belonging to relation, can be constructed according
   *         to description).
   **/
  static bool DescriptionIsValid(const CatalogRelationSchema &relation,
                                 const IndexSubBlockDescription &description);

  /**
   * @brief Estimate the average number of bytes (including any applicable
   *        overhead) used to index a single tuple in this type of
   *        IndexSubBlock. Used by StorageBlockLayout::finalize() to divide
   *        block memory amongst sub-blocks.
   * @warning description must be valid. DescriptionIsValid() should be called
   *          first if necessary.
   *
   * @param relation The relation tuples belong to.
   * @param description A description of the parameters for this type of
   *        IndexSubBlock.
   * @return The average/ammortized number of bytes used to index a single
   *         tuple of relation in an IndexSubBlock of this type described by
   *         description.
   **/
  static std::size_t EstimateBytesPerTuple(const CatalogRelationSchema &relation,
                                           const IndexSubBlockDescription &description);

  /**
   * @return The sub block type.
   */
  IndexSubBlockType getIndexSubBlockType() const override {
    return kSMA;
  }

  /**
   * @return \c true if the SMA block is initialized.
   */
  bool supportsAdHocAdd() const override{
    return initialized_;
  }

  /**
   * @return \c true if the SMA block is initialized.
   */
  bool supportsAdHocRemove() const override {
    return initialized_;
  }

  /**
   * @param tuple The new tuple added.
   * @return \c true always. There's no reason we should ever run out of space once
   *         this index has been successfully created
   */
  bool addEntry(const tuple_id tuple) override;

  /**
   * @brief Updates the index to reflect the removal of a single tuple.
   *
   * @param tuple The id of the tuple which is going to be removed (ie it still
   *        exists within the storage block.
   */
  void removeEntry(const tuple_id tuple) override;

  /**
   * @brief Updates the index to reflect the addition of several tuples.
   *
   * @param tuples The ids of the tuple which have been added (ie they exist within
   *        the storage block.)
   * @return \c true if successful.
   */
  bool bulkAddEntries(const TupleIdSequence &tuples) override;

  /**
   * @brief Updates the index to reflect the removal of several tuples.
   *
   * @param tuples The ids of the tuple which is going to be removed (ie they
   *        still exist within the storage block.
   */
  void bulkRemoveEntries(const TupleIdSequence &tuples) override;

  /**
   * @brief Gives an estimate of how long it will take to respond to a query.
   * The SMA index will detect if one of the following cases is true:
   *   1) Complete match: all the tuples in this subblock will match the predicate
   *   2) Empty match:    none of the tuples will match
   *   3) Partial match:  some of the tuples may match
   * If there is a partial match, the SMA index is of no use. However, in a
   * Complete or Empty match, the SMA index can speed up the selection process
   * and should be used.
   *
   * @param predicate A simple predicate too be evaluated.
   * @return The cost associated with the type of match. Empty matches are constant,
   *         partial matches require a scan, and complete matches require something
   *         less than a regular scan (because we don't need to do the comparison
   *         operation for each tuple) but is still linear with the number of tuples.
   */
  predicate_cost_t estimatePredicateEvaluationCost(
      const ComparisonPredicate &predicate) const override;

  /**
   * @warning Calling this method on the SMA index implies that we are not going
   *          to do a scan for some tuple matches. As in, the SMA index will
   *          either return an empty set of tuple ids, or a set of tuple ids
   *          which is the entire set of all tuple ids in the storage subblock.
   * @note Currently this version only supports simple comparisons of a literal
   *       value with a non-composite key.
   **/
  TupleIdSequence* getMatchesForPredicate(const ComparisonPredicate &predicate,
                                          const TupleIdSequence *filter) const override;

  /**
   * @brief Update the index to reflect the current state of the storage block.
   *
   * @return \c true if successful.
   */
  bool rebuild() override;

  /**
   * @brief Returns if the index is consistent. Rebuilding will ensure this
   *        returns true.
   *
   * @return \c true if inconsistent (rebuild to return true).
   */
  bool requiresRebuild() const;

  /**
   * @brief Given an attribute, quickly check to see if the SMA index contains an
   *        entry for it.
   *
   * @param attribute The ID of the attribute to check.
   * @return \c true if this index contains an entry.
   */
  bool hasEntryForAttribute(attribute_id attribute) const;

  /**
   * @brief Returns the number of tuples, the aggregate COUNT, of the storage sub block.
   *
   * @return Number of tuples in the sub block.
   */
  std::uint32_t getCount() const {
    return reinterpret_cast<sma_internal::SMAHeader*>(sub_block_memory_)->count_;
  }

 private:
  void solvePredicate(sma_internal::SMAPredicate &predicate) const;

  // Retrieves an entry, first checking if the given attribute is indexed.
  inline const sma_internal::SMAEntry* getEntryChecked(attribute_id attribute) const {
    if (attribute_to_entry_.find(attribute) != attribute_to_entry_.end()) {
      return getEntry(attribute);
    }
    return nullptr;
  }

  // Retrieves an entry, not checking if the given attribute is indexed.
  // Warning: This should not be used unless the attribute is indexed.
  inline const sma_internal::SMAEntry* getEntry(attribute_id attribute) const {
    return (entries_ + attribute_to_entry_.at(attribute));
  }

  // Resets a single entry to a zero and invalid state.
  // This should be called before rebuilding, or while initializing.
  void resetEntry(sma_internal::SMAEntry *entry, attribute_id attribute, const Type &attribute_type);

  // Sets all entries to a zero'd and invalid state.
  // This is called prior to a rebuild.
  void resetEntries();

  void addTuple(tuple_id tuple);

  // Frees any TypedValue data which is held in the SMA entries.
  void freeOutOfLineData();

  sma_internal::SMAHeader *header_;
  sma_internal::SMAEntry *entries_;
  std::unordered_map<attribute_id, int> attribute_to_entry_;
  int indexed_attributes_;
  bool initialized_;

  // Maps AttributeTypeID -> addOperator. The addOperator takes the attribute
  // TypedValue and a TypedValue of the SumType on the right.
  // So when using, it should look like:
  //    add_operations_.at(attribute_typeid).applyWithTypedValues(
  //           Attribute Type TypedValue,
  //           SumType TypedValue);
  // TODO(marc): could replace this with an array as it would make lookups
  //             faster without using much space.
  std::unordered_map<int, UncheckedBinaryOperator*> add_operations_;

  // Maps AttributeTypeID -> ComparisonOperator. The Comparison operator
  // must be used with 2 Typed Values of the same type as the Attribute Type.
  // So when using, it should look like:
  //    add_operations_.at(attribute_typeid).applyWithTypedValues(
  //           Attribute Type TypedValue,
  //           Attribute Type TypedValue);
  // TODO(marc): could replace this with an array as it would make lookups
  //             faster without using much space.
  std::unordered_map<int, UncheckedComparator*> less_comparisons_;
  std::unordered_map<int, UncheckedComparator*> equal_comparisons_;

  friend class SMAIndexSubBlockTest;

  DISALLOW_COPY_AND_ASSIGN(SMAIndexSubBlock);

};
} /* namespace quickstep */

#endif /* QUICKSTEP_STORAGE_SMA_INDEX_SUB_BLOCK_HPP_ */