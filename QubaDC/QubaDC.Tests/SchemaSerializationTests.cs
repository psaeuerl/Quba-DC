using QubaDC.DatabaseObjects;
using QubaDC.Utility;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace QubaDC.Tests
{
    public class SchemaSerializationTests
    {
        [Fact]
        public void SerDeWorks()
        {
            Schema x = new Schema()
            {
                Tables = new Table[]
                {

                          new Table("schema","name","col1","col2")
                }
            };

            String ser = JsonSerializer.SerializeObject(x);
            Assert.NotNull(ser);
            Schema y = JsonSerializer.DeserializeObject<Schema>(ser);
            Assert.NotNull(y);
            //TODO => equality check
        }
    }
}
