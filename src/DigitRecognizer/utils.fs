[<AutoOpen>]
module DigitRecognizer.Utils

open System
open System.IO
open System.Text

open Nessos.Streams

open MBrace.Core

type TrainingImage with

    /// Parses a training set from text using the Kaggle digit recognizer CSV format
    static member Parse(file : string) : TrainingImage [] =
        File.ReadAllLines(file)
        |> Stream.ofSeq
        |> Stream.skip 1
        |> Stream.map (fun line -> line.Split(','))
        |> Stream.map (fun line -> line |> Array.map int)
        |> Stream.mapi (fun i nums -> 
                            let id = i + 1
                            let image = { Id = id ; Pixels = nums.[1..] }
                            { Classification = nums.[0] ; Image = image })
        |> Stream.toArray


type Image with

    /// Parses a set of points from text using the Kaggle digit recognizer CSV format
    static member Parse (file : string) : Image [] =
        File.ReadAllLines(file)
        |> Stream.ofSeq
        |> Stream.skip 1
        |> Stream.map (fun line -> line.Split(','))
        |> Stream.map (fun line -> line |> Array.map int)
        |> Stream.mapi (fun i nums -> let id = i + 1 in { Id = id ; Pixels = nums })
        |> Stream.toArray

type Classifications =

    /// Writes a point classification to file
    static member Write(outFile : string, classifications : (ImageId * Classification) []) =
        let fs = File.OpenWrite(outFile)
        use sw = new StreamWriter(fs) 
        sw.WriteLine "ImageId,Label"
        classifications |> Array.iter (fun (i,c) -> sw.WriteLine(sprintf "%d,%d" i c))