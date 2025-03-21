/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "flex/engines/graph_db/runtime/utils/expr.h"

namespace gs {

namespace runtime {

RTAny Expr::eval_path(size_t idx, Arena& arena) const {
  RTAny ret = expr_->eval_path(idx, arena);
  return ret;
}

RTAny Expr::eval_vertex(label_t label, vid_t v, size_t idx,
                        Arena& arena) const {
  return expr_->eval_vertex(label, v, idx, arena);
}
RTAny Expr::eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                      const Any& data, size_t idx, Arena& arena) const {
  return expr_->eval_edge(label, src, dst, data, idx, arena);
}

RTAny Expr::eval_path(size_t idx, Arena& arena, int) const {
  return expr_->eval_path(idx, arena, 0);
}

RTAny Expr::eval_vertex(label_t label, vid_t v, size_t idx, Arena& arena,
                        int) const {
  return expr_->eval_vertex(label, v, idx, arena, 0);
}

RTAny Expr::eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                      const Any& data, size_t idx, Arena& arena, int) const {
  return expr_->eval_edge(label, src, dst, data, idx, arena, 0);
}

RTAnyType Expr::type() const { return expr_->type(); }

}  // namespace runtime

}  // namespace gs