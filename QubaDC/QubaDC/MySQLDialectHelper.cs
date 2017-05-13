using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC
{
    public class MySQLDialectHelper
    {
        internal static string RenderDateTime(DateTime dateTime)
        {
            String format = "TIMESTAMP '{0:D4}-{1:D2}-{2:D2} {3:D2}:{4:D2}:{5:D2}.{6}'";
            String result = String.Format(format, dateTime.Year, dateTime.Month, dateTime.Day
                 , dateTime.Hour, dateTime.Minute, dateTime.Second, dateTime.Millisecond);
            return result;
        }
    }
}
