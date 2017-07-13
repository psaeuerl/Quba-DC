using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace QubaDC.Tests.CustomAsserts
{
    public class QueryStoreAsserts
    {
        public static void ReexcuteIsCorrect(QueryStoreSelectResult result, QueryStoreReexecuteResult result2)
        {
            Assert.Equal(result2.Hash, result.Hash);

            result.Result.TableName = "h1";
            StringWriter wt = new StringWriter();
            result.Result.WriteXml(wt);
            wt.Flush();
            String r1Data = wt.ToString();

            result2.Result.TableName = "h1";
            StringWriter wt2 = new StringWriter();
            result2.Result.WriteXml(wt2);
            wt2.Flush();
            String r2Data = wt2.ToString();
            Assert.Equal(r1Data, r2Data);

        }
    }
}
