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

import io.trino.spi.TrinoException;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;

import java.util.function.Function;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

public final class SparkExpressions
{
    private static final BaseErrorListener ERROR_LISTENER = new BaseErrorListener()
    {
        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String message, RecognitionException e)
        {
            throw new ParsingException(message, e, line, charPositionInLine + 1);
        }
    };

    private SparkExpressions() {}

    public static String toTrinoExpression(String sparkExpression)
    {
        try {
            Expression expression = createExpression(sparkExpression);
            return SparkExpressionConverter.toTrinoExpression(expression);
        }
        catch (ParsingException e) {
            throw new TrinoException(NOT_SUPPORTED, "Unsupported Spark expression: " + sparkExpression, e);
        }
    }

    private static Expression createExpression(String expressionPattern)
    {
        return (Expression) invokeParser(expressionPattern, SparkExpressionParser::standaloneExpression);
    }

    private static Object invokeParser(String input, Function<SparkExpressionParser, ParserRuleContext> parseFunction)
    {
        try {
            SparkExpressionLexer lexer = new SparkExpressionLexer(new CaseInsensitiveStream(CharStreams.fromString(input)));
            CommonTokenStream tokenStream = new CommonTokenStream(lexer);
            SparkExpressionParser parser = new SparkExpressionParser(tokenStream);

            lexer.removeErrorListeners();
            lexer.addErrorListener(ERROR_LISTENER);

            parser.removeErrorListeners();
            parser.addErrorListener(ERROR_LISTENER);

            ParserRuleContext tree;
            try {
                // first, try parsing with potentially faster SLL mode
                parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
                tree = parseFunction.apply(parser);
            }
            catch (ParseCancellationException ex) {
                // if we fail, parse with LL mode
                tokenStream.seek(0); // rewind input stream
                parser.reset();

                parser.getInterpreter().setPredictionMode(PredictionMode.LL);
                tree = parseFunction.apply(parser);
            }
            return new SparkExpressionBuilder().visit(tree);
        }
        catch (StackOverflowError e) {
            throw new IllegalArgumentException("expression pattern is too large (stack overflow while parsing)");
        }
    }
}
