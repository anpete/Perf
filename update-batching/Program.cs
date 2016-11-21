using System;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Text;

namespace ConsoleApplication
{
    public class Program
    {
        private const string ConnectionString 
            = "Data Source=(localdb)\\MSSQLLocalDB;Database=Fortunes;Integrated Security=True";

        private const int Iterations = 50000;
       
        public static void Main(string[] args)
        {
            Batching_multiple_readers();
            Batching_single_reader();
            Batching_execute_scalar();
            Batching_execute_non_query();
            Batching_execute_non_query_in_out();
        }

        private static void PrintResults(string name, long total)
        {
            Console.WriteLine();
            Console.WriteLine($"-- {name} --");
            Console.WriteLine($"Total ticks: {total}");
            Console.WriteLine($"Average ticks: {(double)total / Iterations}");
        }

        public static void Batching_multiple_readers()
        {
            var total = 0L;

            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();

                using (var command = CreateMultiReaderCommand(connection))
                {
                    var sw = new Stopwatch();

                    for (var i = 0; i < Iterations; i++)
                    {
                        sw.Start();

                        using (var reader = command.ExecuteReader())
                        {
                            do
                            {
                                reader.Read();
                            }
                            while (reader.NextResult());
                        }

                        sw.Stop();

                        total += sw.ElapsedTicks;

                        sw.Reset();
                    }

                    PrintResults("Multiple Readers", total);
                }
            }
        }
        
        private static DbCommand CreateMultiReaderCommand(SqlConnection connection)
        {
            var command = connection.CreateCommand();
            var random = new Random();

            var sb = new StringBuilder();

            for (var i = 0; i < 20; i += 2)
            {
                command.Parameters.AddWithValue($"p{i}", random.Next(1, 10001));
                command.Parameters.AddWithValue($"p{i + 1}", random.Next(1, 10001));

                sb.AppendLine($"UPDATE [world] SET [randomnumber] = @p{i} WHERE [id] = @p{i + 1};");
                sb.AppendLine("SELECT @@ROWCOUNT;");
            }

            command.CommandText = sb.ToString();

            return command;
        }

        public static void Batching_single_reader()
        {
            var total = 0L;

            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();

                using (var command = CreateSingleReaderCommand(connection))
                {
                    var sw = new Stopwatch();

                    for (var i = 0; i < Iterations; i++)
                    {
                        sw.Start();

                        using (var reader = command.ExecuteReader())
                        {
                            reader.Read();
                        }

                        sw.Stop();

                        total += sw.ElapsedTicks;

                        sw.Reset();
                    }

                    PrintResults("Single Reader", total);
                }
            }
        }

        public static void Batching_execute_scalar()
        {
            var total = 0L;

            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();

                using (var command = CreateSingleReaderCommand(connection))
                {
                    var sw = new Stopwatch();

                    for (var i = 0; i < Iterations; i++)
                    {
                        sw.Start();

                        command.ExecuteScalar();   

                        sw.Stop();

                        total += sw.ElapsedTicks;

                        sw.Reset();
                    }
                }

                PrintResults("Execute Scalar", total);
            }
        }

        private static DbCommand CreateSingleReaderCommand(SqlConnection connection)
        {
            var command = connection.CreateCommand();
            var random = new Random();

            var sb = new StringBuilder();

            sb.AppendLine("DECLARE @rowsAffected INT = 0;");

            var mask = 1;

            for (var i = 0; i < 20; i += 2)
            {
                command.Parameters.AddWithValue($"p{i}", random.Next(1, 10001));
                command.Parameters.AddWithValue($"p{i + 1}", random.Next(1, 10001));

                sb.AppendLine($"UPDATE [world] SET [randomnumber] = @p{i} WHERE [id] = @p{i + 1};");
                sb.AppendLine($"SET @rowsAffected = @rowsAffected ^ {mask}");

                mask = mask << 1;
            }

            sb.AppendLine("SELECT @rowsAffected;");

            command.CommandText = sb.ToString();

            return command;
        }

        public static void Batching_execute_non_query()
        {
            var total = 0L;

            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();

                using (var command = CreateNonQueryCommand(connection))
                {
                    var sw = new Stopwatch();

                    for (var i = 0; i < Iterations; i++)
                    {
                        sw.Start();

                        command.ExecuteNonQuery();

                        sw.Stop();

                        total += sw.ElapsedTicks;

                        sw.Reset();
                    }
                }

                PrintResults("NonQuery (Out param)", total);
            }
        }

        private static DbCommand CreateNonQueryCommand(SqlConnection connection)
        {
            var command = connection.CreateCommand();

            var parameter = command.CreateParameter();

            parameter.ParameterName = "rowsAffected";
            parameter.Direction = ParameterDirection.Output;
            parameter.DbType = DbType.Int32;

            command.Parameters.Add(parameter);

            var random = new Random();
            var sb = new StringBuilder();
            var mask = 1;

            sb.AppendLine("SET @rowsAffected = 0");

            for (var i = 0; i < 20; i += 2)
            {
                command.Parameters.AddWithValue($"p{i}", random.Next(1, 10001));
                command.Parameters.AddWithValue($"p{i + 1}", random.Next(1, 10001));

                sb.AppendLine($"UPDATE [world] SET [randomnumber] = @p{i} WHERE [id] = @p{i + 1};");
                sb.AppendLine($"SET @rowsAffected = @rowsAffected ^ {mask}");

                mask = mask << 1;
            }

            command.CommandText = sb.ToString();

            return command;
        }

        public static void Batching_execute_non_query_in_out()
        {
            var total = 0L;

            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();

                using (var command = CreateNonQueryInOutCommand(connection))
                {
                    var sw = new Stopwatch();

                    for (var i = 0; i < Iterations; i++)
                    {
                        sw.Start();

                        command.Parameters[0].Value = 0;
                        command.ExecuteNonQuery();

                        sw.Stop();

                        total += sw.ElapsedTicks;

                        sw.Reset();
                    }
                }

                PrintResults("NonQuery (InOut param)", total);
            }
        }

        private static DbCommand CreateNonQueryInOutCommand(SqlConnection connection)
        {
            var command = connection.CreateCommand();

            var parameter = command.CreateParameter();

            parameter.ParameterName = "rowsAffected";
            parameter.Direction = ParameterDirection.InputOutput;
            parameter.DbType = DbType.Int32;

            command.Parameters.Add(parameter);

            var random = new Random();
            var sb = new StringBuilder();
            var mask = 1;

            for (var i = 0; i < 20; i += 2)
            {
                command.Parameters.AddWithValue($"p{i}", random.Next(1, 10001));
                command.Parameters.AddWithValue($"p{i + 1}", random.Next(1, 10001));

                sb.AppendLine($"UPDATE [world] SET [randomnumber] = @p{i} WHERE [id] = @p{i + 1};");
                sb.AppendLine($"SET @rowsAffected = @rowsAffected ^ {mask}");

                mask = mask << 1;
            }

            command.CommandText = sb.ToString();

            return command;
        }
    }
}
