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
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestSparkExpressions
{
    @Test
    public void testBoolean()
    {
        assertExpression("a = true", "(\"a\" = true)");
    }

    @Test
    public void testString()
    {
        // Spark supports both ' and " for string literals
        assertExpression("a = 'quote'", "(\"a\" = 'quote')");
        assertExpression("a = 'a''quote'", "(\"a\" = 'a''quote')");
        assertExpression("a = \"double-quote\"", "(\"a\" = 'double-quote')");
        assertExpression("a = \"a\"\"double-quote\"", "(\"a\" = 'a\"double-quote')");
    }

    @Test
    public void testNumber()
    {
        assertExpression("a = -1", "(\"a\" = -1)");
    }

    @Test
    public void testEquals()
    {
        assertExpression("a = 1", "(\"a\" = 1)");
        assertExpression("a = 'test'", "(\"a\" = 'test')");
    }

    @Test
    public void testNotEquals()
    {
        assertExpression("a <> 1", "(\"a\" <> 1)");
        assertExpression("a != 1", "(\"a\" <> 1)");
    }

    @Test
    public void testLessThan()
    {
        assertExpression("a < 1", "(\"a\" < 1)");
    }

    @Test
    public void testLessThanOrEquals()
    {
        assertExpression("a <= 1", "(\"a\" <= 1)");
    }

    @Test
    public void testGraterThan()
    {
        assertExpression("a > 1", "(\"a\" > 1)");
    }

    @Test
    public void testGraterThanOrEquals()
    {
        assertExpression("a >= 1", "(\"a\" >= 1)");
    }

    @Test
    public void testAnd()
    {
        assertExpression("a > 1 AND a < 10", "((\"a\" > 1) AND (\"a\" < 10))");
    }

    @Test
    public void testIdentifier()
    {
        // Spark uses ` for identifiers
        assertExpression("`123` = 1", "(\"123\" = 1)");
        assertExpression("`a.dot` = 1", "(\"a.dot\" = 1)");
        assertExpression("`a``backtick` = 1", "(\"a`backtick\" = 1)");
    }

    @Test
    public void testInvalidNotBoolean()
    {
        assertParseFailure("a + a");
    }

    // TODO: Support following expressions

    @Test
    public void testUnsupportedRawStringLiteral()
    {
        assertParseFailure("a = r'Some\nText'");
    }

    @Test
    public void testUnsupportedNot()
    {
        assertParseFailure("NOT a = 1");
    }

    @Test
    public void testUnsupportedOr()
    {
        assertParseFailure("a = 1 OR a = 2");
    }

    @Test
    public void testUnsupportedOperator()
    {
        assertParseFailure("a <=> 1");
        assertParseFailure("a == 1");
        assertParseFailure("a = b % 1");
        assertParseFailure("a = b & 1");
        assertParseFailure("a = b * 1");
        assertParseFailure("a = b + 1");
        assertParseFailure("a = b - 1");
        assertParseFailure("a = b / 1");
        assertParseFailure("a = b ^ 1");
        assertParseFailure("a = b::INTEGER");
        assertParseFailure("a = json_column:root");
        assertParseFailure("a BETWEEN 1 AND 10");
        assertParseFailure("a IS NULL");
        assertParseFailure("a IS DISTINCT FROM b");
        assertParseFailure("a IS true");
        assertParseFailure("a = 'Spark' || 'SQL'");
        assertParseFailure("a = 3 | 5");
        assertParseFailure("a = ~ 0");
        assertParseFailure("a = cast(TIMESTAMP'1970-01-01 00:00:01' AS LONG)");
    }

    @Test
    public void testUnsupportedTypes()
    {
        assertParseFailure("a = 123.456");
        assertParseFailure("a = x'123456'");
        assertParseFailure("a = date '2021-01-01'");
        assertParseFailure("a = timestamp '2021-01-01 00:00:00'");
        assertParseFailure("a = array(1, 2, 3)");
        assertParseFailure("a = map(1.0, '2', 3.0, '4')");
        assertParseFailure("a = struct(1, 2, 3)");

        assertParseFailure("a[0] = 1");
    }

    @Test
    public void testUnsupportedCallFunction()
    {
        assertParseFailure("a = abs(-1)");
    }

    private static void assertExpression(@Language("SQL") String sparkExpression, @Language("SQL") String trinoExpression)
    {
        assertEquals(toTrinoExpression(sparkExpression), trinoExpression);
    }

    private static String toTrinoExpression(@Language("SQL") String sparkExpression)
    {
        return SparkExpressions.toTrinoExpression(sparkExpression);
    }

    private static void assertParseFailure(@Language("SQL") String sparkExpression)
    {
        assertThatThrownBy(() -> toTrinoExpression(sparkExpression))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Unsupported Spark expression: " + sparkExpression);
    }
}
