import { projectionRun } from "./run.js";

const snapshot = await projectionRun();
console.log(snapshot.extensions.bookmarks);
