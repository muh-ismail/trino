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
package io.trino.plugin.deltalake.expression;

import static java.util.stream.Collectors.joining;

public final class SparkExpressionConverter
{
    private SparkExpressionConverter() {}

    public static String toTrinoExpression(Expression expression)
    {
        return new Formatter().process(expression, null);
    }

    public static class Formatter
            extends AstVisitor<String, Void>
    {
        @Override
        protected String visitNode(Node node, Void context)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        protected String visitIdentifier(Identifier node, Void context)
        {
            return formatIdentifier(node.getValue());
        }

        private static String formatIdentifier(String s)
        {
            return '"' + s.replace("\"", "\"\"") + '"';
        }

        @Override
        protected String visitStringLiteral(StringLiteral node, Void context)
        {
            return "'" + node.getValue().replace("'", "''") + "'";
        }

        @Override
        protected String visitLiteral(Literal node, Void context)
        {
            return node.getValue();
        }

        @Override
        protected String visitComparisonExpression(ComparisonExpression node, Void context)
        {
            return formatBinaryExpression(node.getOperator().getValue(), node.getLeft(), node.getRight());
        }

        @Override
        protected String visitLogicalExpression(LogicalExpression node, Void context)
        {
            return "(" +
                    node.getTerms().stream()
                            .map(term -> process(term, context))
                            .collect(joining(" " + node.getOperator().toString() + " ")) +
                    ")";
        }

        private String formatBinaryExpression(String operator, Expression left, Expression right)
        {
            return '(' + process(left, null) + ' ' + operator + ' ' + process(right, null) + ')';
        }
    }
}
