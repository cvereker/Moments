using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;

[Serializable]
[Microsoft.SqlServer.Server.SqlUserDefinedAggregate(Format.Native)]
public struct Kurt
{
    private double  n, sum, sum2, sum3, sum4;
    public void Init()
    {
        n = 0;
        sum = sum2 = sum3 = sum4=0;
    }

    public void Accumulate(SqlDouble sqlValue)
    {
        if (sqlValue.IsNull)
            return;

        var value = sqlValue.Value;
        n++;
        sum += value;
        sum2 += (value * value);
        sum3 += (value * value * value);
        sum4 += (value * value * value * value);
    }

    public void Merge (Kurt other)
    {
        n += other.n;
        sum += other.sum;
        sum2 += other.sum2;
        sum3 += other.sum3;
        sum4 += other.sum4;
    }

    public SqlDouble Terminate ()
    {
        var average = sum / n;
        var variance = (sum2 - n * average * average) / (n - 1);
        if (variance <= 0) variance = 0; // allow for negative variance due to numerical error

        if (n > 3)
        {
            return n*(n + 1)/((n - 1)*(n - 2)*(n - 3)*variance*variance)*
                   (sum4 - 4*sum3*average + 6*sum2*average*average - 3*sum*average*average*average)
                   - 3*(n - 1)*(n - 1)/((n - 2)*(n - 3));
        }
        else return SqlDouble.Null;

        
    }
    
}
