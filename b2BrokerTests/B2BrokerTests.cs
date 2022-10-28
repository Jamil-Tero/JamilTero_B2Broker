using JamilTero_B2BrokerClassLibrary;
using System.Collections;

namespace b2BrokerTests
{
    [TestClass]
    public class B2BrokerTests
    {
        [TestMethod]
        public async Task oneInstanceTwoThreads()
        {
            IBusConnection aws = new AWSConnection();
            Logger overallLoger = new Logger(false);
            await using (var awsManager = new Manager(aws, overallLoger, "AWS", 20))
            {
                awsManager.SendMessageAsync(Convert.FromBase64String("a1111111"));
                Task.Run(() =>
                {
                    awsManager.SendMessageAsync(Convert.FromBase64String("b1111111"));
                    awsManager.SendMessageAsync(Convert.FromBase64String("b2222222"));
                    awsManager.SendMessageAsync(Convert.FromBase64String("b3333333"));
                    awsManager.SendMessageAsync(Convert.FromBase64String("b4444444"));
                });
                Thread.Sleep(50);
                awsManager.SendMessageAsync(Convert.FromBase64String("a2222222"));
                awsManager.SendMessageAsync(Convert.FromBase64String("a3333333"));
                awsManager.SendMessageAsync(Convert.FromBase64String("a4444444"));
            }
            await aws.wait();
            Assert.AreEqual("a1111111b1111111b2222222b3333333b4444444a2222222a3333333a4444444", aws.copyLog());
        }
        [TestMethod]
        public async Task TowInstances()
        {
            IBusConnection aws = new AWSConnection();
            IBusConnection mongo = new MongoConnection();
            IBusConnection azure = new AzureConnection();
            Logger overallLoger = new Logger(false);
            await using (var awsManager = new Manager(aws, overallLoger, "AWS", 20))
            await using (var azureManager = new Manager(azure, overallLoger, "Azure", 20))
            {
                awsManager.SendMessageAsync(Convert.FromBase64String("a1111111"));
                azureManager.SendMessageAsync(Convert.FromBase64String("y1111111"));
                Task.Run(() =>
                {
                    awsManager.SendMessageAsync(Convert.FromBase64String("b1111111"));
                    awsManager.SendMessageAsync(Convert.FromBase64String("b2222222"));
                    awsManager.SendMessageAsync(Convert.FromBase64String("b3333333"));
                    azureManager.SendMessageAsync(Convert.FromBase64String("z1111111"));
                    azureManager.SendMessageAsync(Convert.FromBase64String("z2222222"));
                    azureManager.SendMessageAsync(Convert.FromBase64String("z3333333"));
                    awsManager.SendMessageAsync(Convert.FromBase64String("b4444444"));
                    azureManager.SendMessageAsync(Convert.FromBase64String("z4444444"));
                });
                Thread.Sleep(50);
                awsManager.SendMessageAsync(Convert.FromBase64String("a2222222"));
                awsManager.SendMessageAsync(Convert.FromBase64String("a3333333"));
                awsManager.SendMessageAsync(Convert.FromBase64String("a4444444"));
                azureManager.SendMessageAsync(Convert.FromBase64String("y2222222"));
                azureManager.SendMessageAsync(Convert.FromBase64String("y3333333"));
                azureManager.SendMessageAsync(Convert.FromBase64String("y4444444"));
            }
            await aws.wait();
            List<string> expected = new List<string> { "a1111111b1111111b2222222b3333333b4444444a2222222a3333333a4444444"
                , "y1111111z1111111z2222222z3333333z4444444y2222222y3333333y4444444" };
            List<string> actual = new List<string> { aws.copyLog(), azure.copyLog() };
            CollectionAssert.AreEqual(expected, actual, StructuralComparisons.StructuralComparer);
        }
        [TestMethod]
        public async Task oneInstance()
        {
            IBusConnection aws = new AWSConnection();
            Logger overallLoger = new Logger(false);
            await using (var awsManager = new Manager(aws, overallLoger, "AWS", 20))
            {
                awsManager.SendMessageAsync(Convert.FromBase64String("b1111111"));
                awsManager.SendMessageAsync(Convert.FromBase64String("b2222222"));
                awsManager.SendMessageAsync(Convert.FromBase64String("b3333333"));
                awsManager.SendMessageAsync(Convert.FromBase64String("b4444444"));
                awsManager.SendMessageAsync(Convert.FromBase64String("b5555555"));
                awsManager.SendMessageAsync(Convert.FromBase64String("b6666666"));
            }
            Assert.AreEqual("b1111111b2222222b3333333b4444444b5555555b6666666", aws.copyLog());
        }
    }
}