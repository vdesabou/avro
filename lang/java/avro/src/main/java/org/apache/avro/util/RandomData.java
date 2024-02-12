/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.util;

import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import com.github.javafaker.Faker;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.time.LocalDate;

/** Generates schema data as Java objects with random values. */
public class RandomData implements Iterable<Object> {
  public static final String USE_DEFAULT = "use-default";

  private static final int MILLIS_IN_DAY = (int) Duration.ofDays(1).toMillis();

  private final Schema root;
  private final long seed;
  private final int count;
  private final boolean utf8ForString;
  private final Faker faker = new Faker();

  private int intCounter = 0;
  private long longCounter = 0L;

  public RandomData(Schema schema, int count) {
    this(schema, count, false);
  }

  public RandomData(Schema schema, int count, long seed) {
    this(schema, count, seed, false);
  }

  public RandomData(Schema schema, int count, boolean utf8ForString) {
    this(schema, count, System.currentTimeMillis(), utf8ForString);
  }

  public RandomData(Schema schema, int count, long seed, boolean utf8ForString) {
    this.root = schema;
    this.seed = seed;
    this.count = count;
    this.utf8ForString = utf8ForString;
  }

  @Override
  public Iterator<Object> iterator() {
    return new Iterator<Object>() {
      private int n;
      private Random random = new Random(seed);

      @Override
      public boolean hasNext() {
        return n < count;
      }

      @Override
      public Object next() {
        n++;
        return generate(root, random, 0);
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  @SuppressWarnings(value = "unchecked")
  private Object generate(Schema schema, Random random, int d) {
    switch (schema.getType()) {
    case RECORD:
      GenericRecord record = new GenericData.Record(schema);
      for (Schema.Field field : schema.getFields()) {
        Object value = (field.getObjectProp(USE_DEFAULT) == null) ? generate(field.schema(), random, d + 1)
            : GenericData.get().getDefaultValue(field);
        record.put(field.name(), value);
      }
      return record;
    case ENUM:
      List<String> symbols = schema.getEnumSymbols();
      return new GenericData.EnumSymbol(schema, symbols.get(random.nextInt(symbols.size())));
    case ARRAY:
      int length = (random.nextInt(5) + 2) - d;
      @SuppressWarnings("rawtypes")
      GenericArray<Object> array = new GenericData.Array(length <= 0 ? 0 : length, schema);
      for (int i = 0; i < length; i++)
        array.add(generate(schema.getElementType(), random, d + 1));
      return array;
    case MAP:
      length = (random.nextInt(5) + 2) - d;
      Map<Object, Object> map = new HashMap<>(length <= 0 ? 0 : length);
      for (int i = 0; i < length; i++) {
        map.put(randomString(random, 40), generate(schema.getValueType(), random, d + 1));
      }
      return map;
    case UNION:
      List<Schema> types = schema.getTypes();
      return generate(types.get(random.nextInt(types.size())), random, d);
    case FIXED:
      byte[] bytes = new byte[schema.getFixedSize()];
      random.nextBytes(bytes);
      return new GenericData.Fixed(schema, bytes);
    case STRING:
      return randomString(random, 40);
    case BYTES:
      return randomBytes(random, 40, schema.getLogicalType());
    case INT:
      return this.randomInt(random, schema.getLogicalType());
    case LONG:
      return this.randomLong(random, schema.getLogicalType());
    case FLOAT:
      return random.nextFloat();
    case DOUBLE:
      return random.nextDouble();
    case BOOLEAN:
      return random.nextBoolean();
    case NULL:
      return null;
    default:
      throw new RuntimeException("Unknown type: " + schema);
    }
  }

  private static final Charset UTF8 = StandardCharsets.UTF_8;

  private int randomInt(Random random, LogicalType type) {
    if (type instanceof LogicalTypes.TimeMillis) {
      final long MILLIS_IN_DAY = 24 * 60 * 60 * 1000; // Total millis in a day.
      return (int)ThreadLocalRandom.current().nextLong(0, MILLIS_IN_DAY);
    }
    if (type instanceof LogicalTypes.TimeMicros || type instanceof LogicalTypes.TimestampMicros) {
      long currentTimestamp = Instant.now().getEpochSecond();
      long fiveDaysAgo = currentTimestamp - TimeUnit.DAYS.toSeconds(5);

      long randomTimestamp = faker.number().numberBetween(fiveDaysAgo, currentTimestamp);

      return (int)(randomTimestamp*1000000);
    }

    if (type instanceof LogicalTypes.Date) {
      // Create a LocalDate object for January 1, 1970
      LocalDate epoch = LocalDate.of(1970, 1, 1);

      // Get the current date
      LocalDate today = LocalDate.now();

      // Calculate the number of days between the epoch and today
      long daysSinceEpoch = java.time.temporal.ChronoUnit.DAYS.between(epoch, today);

      return (int)(daysSinceEpoch);
    }
    intCounter++;
    return intCounter;
  }

  private long randomLong(Random random, LogicalType type) {
    if (type instanceof LogicalTypes.TimeMillis || type instanceof LogicalTypes.TimestampMillis) {
      long currentTimestamp = Instant.now().getEpochSecond();
      long fiveDaysAgo = currentTimestamp - TimeUnit.DAYS.toSeconds(5);

      long randomTimestamp = faker.number().numberBetween(fiveDaysAgo, currentTimestamp);

      return (randomTimestamp * 1000L);
    }
    if (type instanceof LogicalTypes.TimeMicros || type instanceof LogicalTypes.TimestampMicros) {
      long currentTimestamp = Instant.now().getEpochSecond();
      long fiveDaysAgo = currentTimestamp - TimeUnit.DAYS.toSeconds(5);

      long randomTimestamp = faker.number().numberBetween(fiveDaysAgo, currentTimestamp);

      return (randomTimestamp * 1000000L);
    }
    longCounter++;
    return longCounter;
  }

  private Object randomString(Random random, int maxLength) {
    return faker.name().firstName();
  }

  private ByteBuffer randomBytes(Random rand, int maxLength, LogicalType type) {
    if (type instanceof LogicalTypes.Decimal) {
      byte[] bytes;
      bytes = generateDecimal((LogicalTypes.Decimal) type, rand);
      ByteBuffer buffer = ByteBuffer.wrap(bytes);
      return buffer;
    } else {
      ByteBuffer bytes = ByteBuffer.allocate(rand.nextInt(maxLength));
      ((Buffer) bytes).limit(bytes.capacity());
      rand.nextBytes(bytes.array());
      return bytes;
    }
  }

  private byte[] generateDecimal(LogicalTypes.Decimal decimalLogicalType, Random random) {
      /*
        According to the Avro 1.9.1 spec (http://avro.apache.org/docs/1.9.1/spec.html#Decimal):

        "The decimal logical type represents an arbitrary-precision signed decimal number of the form
      unscaled × 10-scale.

        "A decimal logical type annotates Avro bytes or fixed types. The byte array must contain the
      two's-complement representation of the unscaled integer value in big-endian byte order. The scale
      is fixed, and is specified using an attribute."

        We generate a random decimal here by starting with a value of zero, then repeatedly multiplying
      by 10^15 (15 is the minimum number of significant digits in a double), and adding a new random
      value in the range [0, 10^15) generated using the Random object for this generator. This is done
      until the precision of the current value is equal to or greater than the precision of the logical
      type. At this point, any extra digits (of there should be at most 14) are rounded off from the
      value, a sign is randomly selected, it is converted to big-endian two's-complement representation,
      and returned.
        */
    BigInteger bigInteger = BigInteger.ZERO;
    final long maxIncrementExclusive = 1_000_000_000_000_000L;
    int precision;
    for (precision = 0; precision < decimalLogicalType.getPrecision(); precision += 15) {
      bigInteger = bigInteger.multiply(BigInteger.valueOf(maxIncrementExclusive));
      long increment = (long) (random.nextDouble() * maxIncrementExclusive);
      bigInteger = bigInteger.add(BigInteger.valueOf(increment));
    }
    bigInteger = bigInteger.divide(
        BigInteger.TEN.pow(precision - decimalLogicalType.getPrecision())
    );
    return bigInteger.toByteArray();
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 3 || args.length > 4) {
      System.out.println("Usage: RandomData <schemafile> <outputfile> <count> [codec]");
      System.exit(-1);
    }
    Schema sch = new Schema.Parser().parse(new File(args[0]));
    try (DataFileWriter<Object> writer = new DataFileWriter<>(new GenericDatumWriter<>())) {
      writer.setCodec(CodecFactory.fromString(args.length >= 4 ? args[3] : "null"));
      writer.setMeta("user_metadata", "someByteArray".getBytes(StandardCharsets.UTF_8));
      writer.create(sch, new File(args[1]));

      for (Object datum : new RandomData(sch, Integer.parseInt(args[2]))) {
        writer.append(datum);
      }
    }
  }
}
