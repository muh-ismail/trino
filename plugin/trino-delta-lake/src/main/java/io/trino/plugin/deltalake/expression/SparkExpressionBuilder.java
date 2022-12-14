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

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;

public class SparkExpressionBuilder
        extends SparkExpressionBaseVisitor<Object>
{
    public SparkExpressionBuilder() {}

    @Override
    public Object visitStandaloneExpression(SparkExpressionParser.StandaloneExpressionContext context)
    {
        return visit(context.expression());
    }

    @Override
    public Object visitExpression(SparkExpressionParser.ExpressionContext context)
    {
        return super.visitExpression(context);
    }

    @Override
    public Object visitPredicated(SparkExpressionParser.PredicatedContext context)
    {
        if (context.predicate() != null) {
            return visit(context.predicate());
        }

        return visit(context.valueExpression);
    }

    @Override
    public Object visitComparison(SparkExpressionParser.ComparisonContext context)
    {
        return new ComparisonExpression(
                getComparisonOperator(((TerminalNode) context.comparisonOperator().getChild(0)).getSymbol()),
                (Expression) visit(context.value),
                (Expression) visit(context.right));
    }

    @Override
    public Node visitAnd(SparkExpressionParser.AndContext context)
    {
        List<ParserRuleContext> terms = flatten(context, element -> {
            if (element instanceof SparkExpressionParser.AndContext) {
                SparkExpressionParser.AndContext and = (SparkExpressionParser.AndContext) element;
                return Optional.of(and.booleanExpression());
            }

            return Optional.empty();
        });

        return new LogicalExpression(LogicalExpression.Operator.AND, visit(terms, Expression.class));
    }

    private static List<ParserRuleContext> flatten(ParserRuleContext root, Function<ParserRuleContext, Optional<List<? extends ParserRuleContext>>> extractChildren)
    {
        List<ParserRuleContext> result = new ArrayList<>();
        Deque<ParserRuleContext> pending = new ArrayDeque<>();
        pending.push(root);

        while (!pending.isEmpty()) {
            ParserRuleContext next = pending.pop();

            Optional<List<? extends ParserRuleContext>> children = extractChildren.apply(next);
            if (!children.isPresent()) {
                result.add(next);
            }
            else {
                for (int i = children.get().size() - 1; i >= 0; i--) {
                    pending.push(children.get().get(i));
                }
            }
        }

        return result;
    }

    @Override
    public Object visitColumnReference(SparkExpressionParser.ColumnReferenceContext context)
    {
        return visit(context.identifier());
    }

    private static ComparisonExpression.Operator getComparisonOperator(Token symbol)
    {
        switch (symbol.getType()) {
            case SparkExpressionLexer.EQ:
                return ComparisonExpression.Operator.EQUAL;
            case SparkExpressionLexer.NEQ:
                return ComparisonExpression.Operator.NOT_EQUAL;
            case SparkExpressionLexer.LT:
                return ComparisonExpression.Operator.LESS_THAN;
            case SparkExpressionLexer.LTE:
                return ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
            case SparkExpressionLexer.GT:
                return ComparisonExpression.Operator.GREATER_THAN;
            case SparkExpressionLexer.GTE:
                return ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
        }

        throw new IllegalArgumentException("Unsupported operator: " + symbol.getText());
    }

    @Override
    public Object visitBooleanLiteral(SparkExpressionParser.BooleanLiteralContext context)
    {
        return new Literal(context.getText());
    }

    @Override
    public Node visitIntegerLiteral(SparkExpressionParser.IntegerLiteralContext context)
    {
        return new Literal(context.getText());
    }

    @Override
    public Object visitBasicStringLiteral(SparkExpressionParser.BasicStringLiteralContext context)
    {
        String token = context.getText();
        if (token.startsWith("\"") && token.endsWith("\"")) {
            token = token.substring(1, token.length() - 1)
                    .replace("\"\"", "\"");
        }
        if (token.startsWith("'") && token.endsWith("'")) {
            token = token.substring(1, token.length() - 1)
                    .replace("''", "'");
        }
        return new StringLiteral(token);
    }

    @Override
    public Node visitUnquotedIdentifier(SparkExpressionParser.UnquotedIdentifierContext context)
    {
        return new Identifier(context.getText());
    }

    @Override
    public Object visitBackQuotedIdentifier(SparkExpressionParser.BackQuotedIdentifierContext context)
    {
        String token = context.getText();
        String identifier = token.substring(1, token.length() - 1)
                .replace("``", "`");
        return new Identifier(identifier);
    }

    private <T> List<T> visit(List<? extends ParserRuleContext> contexts, Class<T> expected)
    {
        return contexts.stream()
                .map(context -> this.visit(context, expected))
                .collect(toImmutableList());
    }

    private <T> T visit(ParserRuleContext context, Class<T> expected)
    {
        return expected.cast(super.visit(context));
    }

    // default implementation is error-prone
    @Override
    protected Object aggregateResult(Object aggregate, Object nextResult)
    {
        if (nextResult == null) {
            throw new UnsupportedOperationException("not yet implemented");
        }
        if (aggregate == null) {
            return nextResult;
        }
        throw new UnsupportedOperationException(format("Cannot combine %s and %s", aggregate, nextResult));
    }
}
