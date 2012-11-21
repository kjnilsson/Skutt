// Learn more about F# at http://fsharp.net. See the 'F# Tutorial' project
// for more guidance on F# programming.

#load "Library1.fs"
#I "bin/debug"
#r "Skutt"

open ScratchPad
open Skutt
open System


let c = new Uri("amqp://guest:guest@localhost:5672")
let bus = new SkuttBus(c)

bus.Connect()




