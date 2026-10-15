import { readGrantAdd } from "./read-grant.js";

console.log(JSON.stringify(await readGrantAdd(), null, 2));
