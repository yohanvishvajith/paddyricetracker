import { buildModule } from "@nomicfoundation/hardhat-ignition/modules";

const OperationsModule = buildModule("OperationsModule", (m) => {
  const operations = m.contract("Operations");
  return { operations };
});

export default OperationsModule;
