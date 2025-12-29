// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;
contract UserAccounts {
    // --- Farmer structure ---
    struct Farmer {
        string id;
        string nic;
        string fullName;
        string district;
        uint256 totalPaddyFieldArea;
    }

    // --- Collector structure ---
    struct Collector {
        string id;
        string nic;
        string fullName;
        string district;
    }

    // --- Miller structure ---
    struct Miller {
        string id;
        string companyRegisterNumber;
        string companyName;
        string homeAddress;
        string district;
        string contactNumber;
    }

    // --- Wholesaler structure ---
    struct Wholesaler {
        string id;
        string companyRegisterNumber;
        string companyName;
        string homeAddress;
        string district;
        string contactNumber;
    }

    // --- Retailer structure ---
    struct Retailer {
        string id;
        string name;
        string district;
    }

    // --- Brewer structure ---
    struct Brewer {
        string id;
        string companyId;
        string name;
        string homeAddress;
        string district;
        string contactNumber;
    }

    // --- AnimalFood structure ---
    struct AnimalFood {
        string id;
        string companyId;
        string name;
        string homeAddress;
        string district;
        string contactNumber;
    }

    // --- Exporter structure ---
    struct Exporter {
        string id;
        string companyId;
        string name;
        string homeAddress;
        string district;
        string contactNumber;
    }

    // --- Input structs ---
    struct FarmerInput {
        string id;
        string nic;
        string fullName;
        string district;
        uint256 totalPaddyFieldArea;
    }

    struct CollectorInput {
        string id;
        string nic;
        string fullName;
        string district;
    }

    struct MillerInput {
        string id;
        string companyRegisterNumber;
        string companyName;
        string homeAddress;
        string district;
        string contactNumber;
    }

    struct WholesalerInput {
        string id;
        string companyRegisterNumber;
        string companyName;
        string homeAddress;
        string district;
        string contactNumber;
    }

    struct RetailerInput {
        string id;
        string name;
        string district;
    }

    struct BrewerInput {
        string id;
        string companyId;
        string name;
        string homeAddress;
        string district;
        string contactNumber;
    }

    struct AnimalFoodInput {
        string id;
        string companyId;
        string name;
        string homeAddress;
        string district;
        string contactNumber;
    }

    struct ExporterInput {
        string id;
        string companyId;
        string name;
        string homeAddress;
        string district;
        string contactNumber;
    }

    // --- Storage mappings ---
    mapping(string => Farmer) public farmers;
    mapping(string => Collector) public collectors;
    mapping(string => Miller) public millers;
    mapping(string => Wholesaler) public wholesalers;
    mapping(string => Retailer) public retailers;
    mapping(string => Brewer) public brewers;
    mapping(string => AnimalFood) public animalFoods;
    mapping(string => Exporter) public exporters;

    // ID arrays for enumeration
    string[] public farmerIds;
    string[] public millerIds;
    string[] public collectorIds;
    string[] public wholesalerIds;
    string[] public retailerIds;
    string[] public brewerIds;
    string[] public animalFoodIds;
    string[] public exporterIds;

    // --- Events ---
    event FarmerRegistered(
        string id,
        string nic,
        string fullName,
        string district,
        uint256 totalPaddyFieldArea
    );

    event CollectorRegistered(
        string id,
        string nic,
        string fullName,
        string district
    );

    event MillerRegistered(
        string id,
        string companyRegisterNumber,
        string companyName,
        string homeAddress,
        string district,
        string contactNumber
    );

    event WholesalerRegistered(
        string id,
        string companyRegisterNumber,
        string companyName,
        string homeAddress,
        string district,
        string contactNumber
    );

    event RetailerRegistered(
        string id,
        string name,
        string district
    );

    event BrewerRegistered(
        string id,
        string companyId,
        string name,
        string homeAddress,
        string district,
        string contactNumber
    );

    event AnimalFoodRegistered(
        string id,
        string companyId,
        string name,
        string homeAddress,
        string district,
        string contactNumber
    );

    event ExporterRegistered(
        string id,
        string companyId,
        string name,
        string homeAddress,
        string district,
        string contactNumber
    );

    event FarmerUpdated(
        string id,
        string nic,
        string fullName,
        string district,
        uint256 totalPaddyFieldArea
    );

    event CollectorUpdated(
        string id,
        string nic,
        string fullName,
        string district
    );

    event MillerUpdated(
        string id,
        string companyRegisterNumber,
        string companyName,
        string homeAddress,
        string district,
        string contactNumber
    );

    event WholesalerUpdated(
        string id,
        string companyRegisterNumber,
        string companyName,
        string homeAddress,
        string district,
        string contactNumber
    );

    event RetailerUpdated(
        string id,
        string name,
        string district
    );

    event BrewerUpdated(
        string id,
        string companyId,
        string name,
        string homeAddress,
        string district,
        string contactNumber
    );

    event AnimalFoodUpdated(
        string id,
        string companyId,
        string name,
        string homeAddress,
        string district,
        string contactNumber
    );

    event ExporterUpdated(
        string id,
        string companyId,
        string name,
        string homeAddress,
        string district,
        string contactNumber
    );

    // --- Register Farmer ---
    function registerFarmer(FarmerInput calldata input) external {
        require(bytes(farmers[input.id].id).length == 0, "Farmer already registered");

        farmers[input.id] = Farmer({
            id: input.id,
            nic: input.nic,
            fullName: input.fullName,
            district: input.district,
            totalPaddyFieldArea: input.totalPaddyFieldArea
        });

        farmerIds.push(input.id);

        emit FarmerRegistered(
            input.id,
            input.nic,
            input.fullName,
            input.district,
            input.totalPaddyFieldArea
        );
    }

    // --- Register Collector ---
    function registerCollector(CollectorInput calldata input) external {
        require(bytes(collectors[input.id].id).length == 0, "Collector already registered");

        collectors[input.id] = Collector({
            id: input.id,
            nic: input.nic,
            fullName: input.fullName,
            district: input.district
        });

        collectorIds.push(input.id);

        emit CollectorRegistered(
            input.id,
            input.nic,
            input.fullName,
            input.district
        );
    }

    // --- Register Miller ---
    function registerMiller(MillerInput calldata input) external {
        require(bytes(millers[input.id].id).length == 0, "Miller already registered");

        millers[input.id] = Miller({
            id: input.id,
            companyRegisterNumber: input.companyRegisterNumber,
            companyName: input.companyName,
            homeAddress: input.homeAddress,
            district: input.district,
            contactNumber: input.contactNumber
        });

        millerIds.push(input.id);

        emit MillerRegistered(
            input.id,
            input.companyRegisterNumber,
            input.companyName,
            input.homeAddress,
            input.district,
            input.contactNumber
        );
    }

    // --- Register Wholesaler ---
    function registerWholesaler(WholesalerInput calldata input) external {
        require(bytes(wholesalers[input.id].id).length == 0, "Wholesaler already registered");

        wholesalers[input.id] = Wholesaler({
            id: input.id,
            companyRegisterNumber: input.companyRegisterNumber,
            companyName: input.companyName,
            homeAddress: input.homeAddress,
            district: input.district,
            contactNumber: input.contactNumber
        });

        wholesalerIds.push(input.id);

        emit WholesalerRegistered(
            input.id,
            input.companyRegisterNumber,
            input.companyName,
            input.homeAddress,
            input.district,
            input.contactNumber
        );
    }

    // --- Register Retailer ---
    function registerRetailer(RetailerInput calldata input) external {
        require(bytes(retailers[input.id].id).length == 0, "Retailer already registered");

        retailers[input.id] = Retailer({
            id: input.id,
            name: input.name,
            district: input.district
        });

        retailerIds.push(input.id);

        emit RetailerRegistered(
            input.id,
            input.name,
            input.district
        );
    }

    // --- Register Brewer ---
    function registerBrewer(BrewerInput calldata input) external {
        require(bytes(brewers[input.id].id).length == 0, "Brewer already registered");

        brewers[input.id] = Brewer({
            id: input.id,
            companyId: input.companyId,
            name: input.name,
            homeAddress: input.homeAddress,
            district: input.district,
            contactNumber: input.contactNumber
        });

        brewerIds.push(input.id);

        emit BrewerRegistered(
            input.id,
            input.companyId,
            input.name,
            input.homeAddress,
            input.district,
            input.contactNumber
        );
    }

    // --- Register AnimalFood ---
    function registerAnimalFood(AnimalFoodInput calldata input) external {
        require(bytes(animalFoods[input.id].id).length == 0, "AnimalFood already registered");

        animalFoods[input.id] = AnimalFood({
            id: input.id,
            companyId: input.companyId,
            name: input.name,
            homeAddress: input.homeAddress,
            district: input.district,
            contactNumber: input.contactNumber
        });

        animalFoodIds.push(input.id);

        emit AnimalFoodRegistered(
            input.id,
            input.companyId,
            input.name,
            input.homeAddress,
            input.district,
            input.contactNumber
        );
    }

    // --- Register Exporter ---
    function registerExporter(ExporterInput calldata input) external {
        require(bytes(exporters[input.id].id).length == 0, "Exporter already registered");

        exporters[input.id] = Exporter({
            id: input.id,
            companyId: input.companyId,
            name: input.name,
            homeAddress: input.homeAddress,
            district: input.district,
            contactNumber: input.contactNumber
        });

        exporterIds.push(input.id);

        emit ExporterRegistered(
            input.id,
            input.companyId,
            input.name,
            input.homeAddress,
            input.district,
            input.contactNumber
        );
    }

    // --- Update Farmer ---
    function updateFarmer(FarmerInput calldata input) external {
        require(bytes(farmers[input.id].id).length != 0, "Farmer not found");

        farmers[input.id] = Farmer({
            id: input.id,
            nic: input.nic,
            fullName: input.fullName,
            district: input.district,
            totalPaddyFieldArea: input.totalPaddyFieldArea
        });

        emit FarmerUpdated(
            input.id,
            input.nic,
            input.fullName,
            input.district,
            input.totalPaddyFieldArea
        );
    }

    // --- Update Collector ---
    function updateCollector(CollectorInput calldata input) external {
        require(bytes(collectors[input.id].id).length != 0, "Collector not found");

        collectors[input.id] = Collector({
            id: input.id,
            nic: input.nic,
            fullName: input.fullName,
            district: input.district
        });

        emit CollectorUpdated(
            input.id,
            input.nic,
            input.fullName,
            input.district
        );
    }

    // --- Update Miller ---
    function updateMiller(MillerInput calldata input) external {
        require(bytes(millers[input.id].id).length != 0, "Miller not found");

        millers[input.id] = Miller({
            id: input.id,
            companyRegisterNumber: input.companyRegisterNumber,
            companyName: input.companyName,
            homeAddress: input.homeAddress,
            district: input.district,
            contactNumber: input.contactNumber
        });

        emit MillerUpdated(
            input.id,
            input.companyRegisterNumber,
            input.companyName,
            input.homeAddress,
            input.district,
            input.contactNumber
        );
    }

    // --- Update Wholesaler ---
    function updateWholesaler(WholesalerInput calldata input) external {
        require(bytes(wholesalers[input.id].id).length != 0, "Wholesaler not found");

        wholesalers[input.id] = Wholesaler({
            id: input.id,
            companyRegisterNumber: input.companyRegisterNumber,
            companyName: input.companyName,
            homeAddress: input.homeAddress,
            district: input.district,
            contactNumber: input.contactNumber
        });

        emit WholesalerUpdated(
            input.id,
            input.companyRegisterNumber,
            input.companyName,
            input.homeAddress,
            input.district,
            input.contactNumber
        );
    }

    // --- Update Retailer ---
    function updateRetailer(RetailerInput calldata input) external {
        require(bytes(retailers[input.id].id).length != 0, "Retailer not found");

        retailers[input.id] = Retailer({
            id: input.id,
            name: input.name,
            district: input.district
        });

        emit RetailerUpdated(
            input.id,
            input.name,
            input.district
        );
    }

    // --- Update Brewer ---
    function updateBrewer(BrewerInput calldata input) external {
        require(bytes(brewers[input.id].id).length != 0, "Brewer not found");

        brewers[input.id] = Brewer({
            id: input.id,
            companyId: input.companyId,
            name: input.name,
            homeAddress: input.homeAddress,
            district: input.district,
            contactNumber: input.contactNumber
        });

        emit BrewerUpdated(
            input.id,
            input.companyId,
            input.name,
            input.homeAddress,
            input.district,
            input.contactNumber
        );
    }

    // --- Update AnimalFood ---
    function updateAnimalFood(AnimalFoodInput calldata input) external {
        require(bytes(animalFoods[input.id].id).length != 0, "AnimalFood not found");

        animalFoods[input.id] = AnimalFood({
            id: input.id,
            companyId: input.companyId,
            name: input.name,
            homeAddress: input.homeAddress,
            district: input.district,
            contactNumber: input.contactNumber
        });

        emit AnimalFoodUpdated(
            input.id,
            input.companyId,
            input.name,
            input.homeAddress,
            input.district,
            input.contactNumber
        );
    }

    // --- Update Exporter ---
    function updateExporter(ExporterInput calldata input) external {
        require(bytes(exporters[input.id].id).length != 0, "Exporter not found");

        exporters[input.id] = Exporter({
            id: input.id,
            companyId: input.companyId,
            name: input.name,
            homeAddress: input.homeAddress,
            district: input.district,
            contactNumber: input.contactNumber
        });

        emit ExporterUpdated(
            input.id,
            input.companyId,
            input.name,
            input.homeAddress,
            input.district,
            input.contactNumber
        );
    }

    // --- View functions ---
    function getFarmer(string calldata _id) external view returns (Farmer memory) {
        require(bytes(farmers[_id].id).length != 0, "Farmer not found");
        return farmers[_id];
    }

    function getCollector(string calldata _id) external view returns (Collector memory) {
        require(bytes(collectors[_id].id).length != 0, "Collector not found");
        return collectors[_id];
    }

    function getMiller(string calldata _id) external view returns (Miller memory) {
        require(bytes(millers[_id].id).length != 0, "Miller not found");
        return millers[_id];
    }

    function getWholesaler(string calldata _id) external view returns (Wholesaler memory) {
        require(bytes(wholesalers[_id].id).length != 0, "Wholesaler not found");
        return wholesalers[_id];
    }

    function getRetailer(string calldata _id) external view returns (Retailer memory) {
        require(bytes(retailers[_id].id).length != 0, "Retailer not found");
        return retailers[_id];
    }

    function getBrewer(string calldata _id) external view returns (Brewer memory) {
        require(bytes(brewers[_id].id).length != 0, "Brewer not found");
        return brewers[_id];
    }

    function getAnimalFood(string calldata _id) external view returns (AnimalFood memory) {
        require(bytes(animalFoods[_id].id).length != 0, "AnimalFood not found");
        return animalFoods[_id];
    }

    function getExporter(string calldata _id) external view returns (Exporter memory) {
        require(bytes(exporters[_id].id).length != 0, "Exporter not found");
        return exporters[_id];
    }

    // --- Get ID counts ---
    function getFarmerCount() external view returns (uint256) {
        return farmerIds.length;
    }

    function getMillerCount() external view returns (uint256) {
        return millerIds.length;
    }

    function getCollectorCount() external view returns (uint256) {
        return collectorIds.length;
    }

    function getWholesalerCount() external view returns (uint256) {
        return wholesalerIds.length;
    }

    function getRetailerCount() external view returns (uint256) {
        return retailerIds.length;
    }

    function getBrewerCount() external view returns (uint256) {
        return brewerIds.length;
    }

    function getAnimalFoodCount() external view returns (uint256) {
        return animalFoodIds.length;
    }

    function getExporterCount() external view returns (uint256) {
        return exporterIds.length;
    }
}
