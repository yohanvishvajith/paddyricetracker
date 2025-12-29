import { buildModule } from "@nomicfoundation/hardhat-ignition/modules";

const UserAccountsModule = buildModule("UserAccountsModule", (m) => {
  const userAccounts = m.contract("UserAccounts");
  return { userAccounts };
});

export default UserAccountsModule;
