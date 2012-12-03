// Learn more about F# at http://fsharp.net
// See the 'F# Tutorial' project for more help.
open System
open System.Threading
open Skutt.RabbitMq
open Skutt.Test.Messages

let sleep = fun (amt:int) -> Thread.Sleep amt

[<EntryPoint>]
let main argv = 
    printfn "%A" argv
    let conn = new Uri @"amqp://guest:guest@localhost:5672"
    use bus = new SkuttBus(conn)
   
    bus.RegisterMessageType<TestEventOne> (new Uri "http://msg.skutt.net/messages/test_two")
    bus.Connect()
   
    let timer = new Timer (
                            (fun (x) -> 
                                    bus.Publish(new TestEventOne())
                                    printfn "%s" "event"), 
                            null, 0, 1000)

    bus.Subscribe<TestEventOne> ("broadcaster", (fun (e) -> printfn "%s" (e.ToString())))

    Console.ReadKey() |> ignore
    0 // return an integer exit code
