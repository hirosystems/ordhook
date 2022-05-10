const { lookpath } = require("lookpath");
const { spawn } = require('child_process');

(async () => {
  const path = await lookpath("clarinet");
  const dap = spawn(path, ['dap'], {stdio: [process.stdin, process.stdout, process.stderr]});
  await new Promise( (resolve) => {
    dap.on('exit', resolve);
  })
})();
