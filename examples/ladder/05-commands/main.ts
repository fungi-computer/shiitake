import { commandsRun } from "./run.js";

const { added, snapshot } = await commandsRun();
console.log("The command returned", added);
console.log(snapshot.extensions.bookmarks);
