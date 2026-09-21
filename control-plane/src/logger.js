function stamp() {
  return new Date().toISOString().replace("T", " ").replace("Z", "");
}

function info(message, ...args) {
  console.log(`${stamp()} INFO hcp: ${message}`, ...args);
}

function warn(message, ...args) {
  console.warn(`${stamp()} WARNING hcp: ${message}`, ...args);
}

module.exports = { info, warn };
