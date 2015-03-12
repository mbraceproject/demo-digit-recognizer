(*
Entirely local model, using no libraries.
"Simplest possible model": 1-nearest neighbor, euclidean distance.
Quad core i7, end-to-end execution:
Real: 00:05:59.484, CPU: 00:22:08.671, GC gen0: 428, gen1: 423, gen2: 6
*)

(*
Base types: 
* an Image is an array of 28 * 28 grayscale pixels (0..255)
* an Example is an image, with known Label (0 to 9)
* a Benchmark is an image we try to predict, with an ID but no Label
*)

type Image = int []
type Example = { Label: int; Image: Image }
type Benchmark = { ImageId: int; Image: Image }

open System.IO

let dataFolder = __SOURCE_DIRECTORY__ + "/../../data/"

let trainPath = dataFolder + "train.csv"
let testPath = dataFolder +  "test.csv"
let submissionPath = dataFolder + "submission.csv"

// Reading the 50,000 known examples in memory.

let training =
    trainPath
    |> File.ReadAllLines
    |> fun lines -> lines.[1..]
    |> Array.map (fun line -> line.Split ',' |> Array.map int)
    |> Array.map (fun line ->
        { Label = line.[0]; Image = line.[1..] })

// Euclidean distance between 2 images.
let size = 28 * 28
let distance (img1:Image) (img2:Image) =
    let rec compute acc i =
        if i = size then acc
        else
            let d = (img1.[i] - img2.[i]) * (img1.[i] - img2.[i])
            compute (acc + d) (i + 1)
    compute 0 0

// Given an image, find the label of the closest
// image from the 50,000 known training images.
let closest img =
    training
    |> Seq.minBy (fun example -> distance example.Image img)
    |> fun closest -> closest.Label

// Read the 20,000 benchmark images we are trying
// to predict.

let testing =
    testPath
    |> File.ReadAllLines
    |> fun lines -> lines.[1..]
    |> Array.map (fun line -> line.Split ',' |> Array.map int)
    |> Array.mapi (fun i image -> 
        { ImageId = i + 1; Image = image } )

// Create a submission file:
// for each of the 20,000 images, produce
// a predicted label, and save the ImageId
// and prediction into a file.

let createSubmission =
    let predictions =
        testing
        |> Array.Parallel.map (fun test -> 
            sprintf "%i,%i" test.ImageId (closest test.Image))
    [|
        yield "ImageId,Label"
        yield! predictions
    |]
    |> fun predictions -> File.WriteAllLines(submissionPath,predictions)