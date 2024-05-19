/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.jdbc.expression;

import com.google.common.collect.ImmutableList;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.spi.expression.Constant;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;

import java.util.Optional;

import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.constant;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static io.trino.spi.type.TimestampType.MAX_SHORT_PRECISION;

public class RewriteTimestampConstant
        implements ConnectorExpressionRule<Constant, ParameterizedExpression>
{
    private static final Pattern<Constant> PATTERN = constant().with(type().matching(TimestampType.class::isInstance));

    @Override
    public Pattern<Constant> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<ParameterizedExpression> rewrite(Constant constant, Captures captures, RewriteContext<ParameterizedExpression> context)
    {
        if (constant.getValue() == null) {
            return Optional.empty();
        }
        Type type = constant.getType();
        int precision = ((TimestampType) type).getPrecision();
        if (precision <= MAX_SHORT_PRECISION) {
            return Optional.of(new ParameterizedExpression("TIMESTAMP '" + new LongTimestamp((Long) constant.getValue(), 0).toString() + "'", ImmutableList.of()));
        }
        else {
            return Optional.of(new ParameterizedExpression("TIMESTAMP '" + ((LongTimestamp) constant.getValue()).toString() + "'", ImmutableList.of()));
        }
    }
}
