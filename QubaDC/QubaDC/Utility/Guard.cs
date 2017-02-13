using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Utility
{
    public class Guard
    {
        public static void ArgumentNotNullOrWhiteSpace(String value,String name)
        {
            ThrowArgumentException<String>(String.IsNullOrWhiteSpace, value, name, "Argument: {0} was null or whitespace");
        }

        public static void ArgumentNotNull(Object arg,String name)
        {
            ThrowArgumentException<Object>((t)=>t==null, arg, name, "Argument: {0} was null");

        }

        private static void ThrowArgumentException<T>(Func<T,Boolean> pre,T value, String Name, String Messge)
        {
            if(pre(value))
            {
                String msg = String.Format(Messge, Name, value);
                throw new ArgumentException(msg);
            }
        }

        internal static void ArgumentTrueForAll<T>(T[] ElementsToCheck, Func<T, bool> predicate, String message)
        {
            var y = ElementsToCheck.Select(x => new { el = x, b = predicate(x) }).Where(x => x.b == false).ToArray();
            if(y.Length>0)
            {
                String Message = message + " Violated by: " + String.Join(System.Environment.NewLine, y.Select(z => z.el));
                throw new ArgumentException(Message);

            }
        }

        internal static void StateEqual(object expected, object real)
        {
            if(expected != real)
            {
                String msg = String.Format("StateEqual failed, Expected: {0}{1}Actual: {2}", expected.ToString(), System.Environment.NewLine, real.ToString());
                throw new InvalidOperationException(msg);
            }
        }
    }
}
