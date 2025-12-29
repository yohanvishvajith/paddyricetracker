import { promises as fs } from "fs";
import path from "path";

async function exportContract(
  contractName: string,
  moduleName: string,
  outputFileName: string,
  solFileName?: string
) {
  const root = process.cwd();
  const chainId = process.env.CHAIN_ID ?? "31337";
  const moduleContractKey = `${moduleName}#${contractName}`;
  const actualSolFile = solFileName || contractName;
  const fallbackArtifactPath = path.join(
    root,
    "artifacts",
    "contracts",
    `${actualSolFile}.sol`,
    `${contractName}.json`
  );

  const deployDir = path.join(
    root,
    "ignition",
    "deployments",
    `chain-${chainId}`
  );
  const deployedAddressesPath = path.join(deployDir, "deployed_addresses.json");
  const ignitionArtifactPath = path.join(
    deployDir,
    "artifacts",
    `${moduleContractKey}.json`
  );

  let abi: any;
  let address: string | undefined;

  try {
    const deployedRaw = await fs.readFile(deployedAddressesPath, "utf-8");
    const deployed = JSON.parse(deployedRaw);
    address = deployed[moduleContractKey];
  } catch (err) {
    console.warn(
      `Could not read deployed addresses at ${deployedAddressesPath}:`,
      (err as Error).message
    );
  }

  try {
    const ignitionRaw = await fs.readFile(ignitionArtifactPath, "utf-8");
    const ignitionArtifact = JSON.parse(ignitionRaw);
    abi = ignitionArtifact.abi;
    console.log(`Loaded ABI from Ignition artifact: ${ignitionArtifactPath}`);
  } catch (err) {
    console.warn(
      `Ignition artifact not found at ${ignitionArtifactPath}, falling back to Hardhat artifacts.`
    );
    const fallbackRaw = await fs.readFile(fallbackArtifactPath, "utf-8");
    const fallbackArtifact = JSON.parse(fallbackRaw);
    abi = fallbackArtifact.abi;
    console.log(`Loaded ABI from Hardhat artifact: ${fallbackArtifactPath}`);
  }

  if (!abi) {
    throw new Error(`ABI not found for ${contractName}.`);
  }

  const abiOutPath = path.join(root, outputFileName);
  await fs.writeFile(abiOutPath, JSON.stringify(abi, null, 2) + "\n", "utf-8");
  console.log(`✓ Wrote ${contractName} ABI to ${abiOutPath}`);

  if (address) {
    const addressOutPath = path.join(
      root,
      outputFileName.replace(".json", "-address.json")
    );
    await fs.writeFile(
      addressOutPath,
      JSON.stringify({ address }, null, 2) + "\n",
      "utf-8"
    );
    console.log(`✓ Wrote ${contractName} address to ${addressOutPath}`);
  }

  return { abi, address };
}

async function main() {
  console.log("Exporting UserAccounts contract...");
  await exportContract(
    "UserAccounts",
    "UserAccountsModule",
    "user-accounts-abi.json"
  );

  console.log("\nExporting Operations contract...");
  await exportContract(
    "Operations",
    "OperationsModule",
    "operations-abi.json",
    "Storage"
  );

  console.log("\n✅ All ABIs exported successfully!");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
