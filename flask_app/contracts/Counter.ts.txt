import { buildModule } from "@nomicfoundation/hardhat-ignition/modules";

export default buildModule("StorageModule", (m) => {
  const storage = m.contract("Storage");

  // No default initialization required for Storage; return the contract handle
  return { storage };
});
