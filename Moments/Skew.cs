using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;

[Serializable]
[Microsoft.SqlServer.Server.SqlUserDefinedAggregate(Format.Native)]
public struct Skew
{
    private double n, sum, sum2, sum3;

    public void Init()
    {
        n = 0;
        sum = sum2 = sum3 =  0.0;
    }

    public void Accumulate(SqlDouble sqlValue)
    {
        if (sqlValue.IsNull)
            return;

        double value = sqlValue.Value;

        n++;
        sum += value;
        sum2 += (value * value);
        sum3 += (value * value * value);
    }

    public void Merge (Skew other)
    {
        this.n += other.n;
        this.sum += other.sum;
        this.sum2 += other.sum2;
        this.sum3 += other.sum3;
    }

    public SqlDouble Terminate ()
    {
        double average = sum / n;
        double variance = (sum2 - n * average * average) / (n - 1);

        // allow for negative variance due to numerical error
        if (variance <= 0) variance = 0;
        double stdev = Math.Sqrt(variance);

        double skew = (n <= 2) ? double.NaN : n / ((n - 1) * (n - 2) * stdev * variance)
               * (sum3 - n * average * average * average) - (3.0 * n / (n - 2))
               * average / stdev;

        if (double.IsNaN(skew))
            return SqlDouble.Null;
        if (double.IsNegativeInfinity(skew))
            return SqlDouble.MinValue;
        if (double.IsPositiveInfinity(skew))
            return SqlDouble.MaxValue;

        return new SqlDouble(skew);
    }

}
