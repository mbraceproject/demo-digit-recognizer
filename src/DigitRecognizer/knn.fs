module DigitRecognizer.Knn

open System
open System.IO

open Nessos.Streams

[<Literal>]
let pixelLength = 784 // 28 * 28

/// l^2 distance 
let l2 : Distance =
    fun x y ->
        let xp = x.Pixels
        let yp = y.Pixels
        let mutable acc = 0uL
        for i = 0 to pixelLength - 1 do
            acc <- acc + uint64 (pown (xp.[i] - yp.[i]) 2)
        acc

/// single-threaded, stream-based k-nearest neighbour classifier
let knn (d : Distance) (k : int) : Classifier =
    fun (training : TrainingImage []) (img : Image) ->
        training
        |> Stream.ofArray
        |> Stream.sortBy (fun ex -> d ex.Image img)
        |> Stream.take k
        |> Stream.map (fun ex -> ex.Classification)
        |> Stream.countBy id
        |> Stream.maxBy snd
        |> fst

/// local multicore classification
let classifyLocalMulticore (classifier : Classifier) (training : TrainingImage []) (images : Image []) =
    ParStream.ofArray images
    |> ParStream.map (fun img -> img.Id, classifier training img)
    |> ParStream.toArray

/// local multicore validation
let validateLocalMulticore (classifier : Classifier) (training : TrainingImage []) (validation : TrainingImage []) =
    ParStream.ofArray validation
    |> ParStream.map(fun tr -> tr.Classification, classifier training tr.Image)
    |> ParStream.map(fun (expected,prediction) -> if expected = prediction then 1. else 0.)
    |> ParStream.sum
    |> fun results -> results / float validation.Length