// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

contract Operations{
    // --- Initial Paddy records ---
    uint256 public nextPaddyRecordId;
    mapping(uint256 => string) public paddyRecordUserId;
    mapping(uint256 => string) public paddyRecordPaddyType;
    mapping(uint256 => uint256) public paddyRecordQuantity;
    mapping(uint256 => uint256) public paddyRecordDate;
    mapping(uint256 => bool) public paddyRecordStatus;

    // --- Damage records ---
    uint256 public nextDamageId;
    mapping(uint256 => string) public damageUserId;
    mapping(uint256 => string) public damagePaddyType;
    mapping(uint256 => uint256) public damageQuantity;
    mapping(uint256 => uint256) public damageDateTimestamp;
    mapping(uint256 => string) public damageReason;

    // --- Milling records ---
    uint256 public nextMillingRecordId;
    mapping(uint256 => uint256) public millingInputPaddy;
    mapping(uint256 => uint256) public millingOutputRice;
    mapping(uint256 => uint256) public millingDateTime;
    mapping(uint256 => string) public millingPaddyType;
    mapping(uint256 => uint256) public millingDryingDuration;
    mapping(uint256 => bool) public millingStatus;

    // --- Initial Rice records ---
    uint256 public nextInitialRiceRecordId;
    mapping(uint256 => string) public initialRiceRecordUserId;
    mapping(uint256 => string) public initialRiceRecordRiceType;
    mapping(uint256 => uint256) public initialRiceRecordQuantity;
    mapping(uint256 => uint256) public initialRiceRecordDate;
    mapping(uint256 => bool) public initialRiceRecordStatus;

    // --- Rice Damage records ---
    uint256 public nextRiceDamageId;
    mapping(uint256 => string) public riceDamageUserId;
    mapping(uint256 => string) public riceDamageType;
    mapping(uint256 => uint256) public riceDamageQuantity;
    mapping(uint256 => uint256) public riceDamageDateTimestamp;
    mapping(uint256 => string) public riceDamageReason;

    // --- Transaction tracking ---
    uint256 public nextTxId;
    mapping(uint256 => string) public txFromParty;
    mapping(uint256 => string) public txToParty;
    mapping(uint256 => string) public txProductType;
    mapping(uint256 => uint256) public txQuantity;
    mapping(uint256 => uint256) public txPrice;
    mapping(uint256 => uint256) public txTimestamp;
    mapping(uint256 => bool) public txStatus;

    // Optional indexes for traceability per actor
    mapping(string => uint256[]) public sentTxs;    // sender name -> txIds
    mapping(string => uint256[]) public receivedTxs; // receiver name -> txIds

    // --- Rice Transaction tracking ---
    uint256 public nextRiceTxId;
    mapping(uint256 => string) public riceTxFromParty;
    mapping(uint256 => string) public riceTxToParty;
    mapping(uint256 => string) public riceTxRiceType;
    mapping(uint256 => uint256) public riceTxQuantity;
    mapping(uint256 => uint256) public riceTxPrice;
    mapping(uint256 => uint256) public riceTxTimestamp;
    mapping(uint256 => bool) public riceTxStatus;

    // Optional indexes for rice transaction traceability per actor
    mapping(string => uint256[]) public sentRiceTxs;    // sender name -> riceTxIds
    mapping(string => uint256[]) public receivedRiceTxs; // receiver name -> riceTxIds

    // --- Events ---
    event InitialPaddyRecorded(
        uint256 indexed recordId,
        string userId,
        string paddyType,
        uint256 quantity,
        uint256 date,
        bool status
    );

    event InitialRiceRecorded(
        uint256 indexed recordId,
        string userId,
        string riceType,
        uint256 quantity,
        uint256 date,
        bool status
    );

    event DamageRecorded(
        uint256 indexed damageId,
        string userId,
        string paddyType,
        uint256 quantity,
        uint256 damageDate,
        string reason
    );
    
    event MillingRecorded(
        uint256 indexed millingId,
        uint256 inputPaddy,
        uint256 outputRice,
        uint256 dateTime,
        string paddyType,
        uint256 dryingDuration,
        bool status
    );
    
    event RiceDamageRecorded(
        uint256 indexed riceDamageId,
        string userId,
        string riceType,
        uint256 quantity,
        uint256 damageDate,
        string reason
    );
    
    // Log for off-chain traceability
    event TransactionRecorded(
        uint256 indexed txId,
        string indexed fromParty,
        string indexed toParty,
        string productType,
        uint256 quantity,
        uint256 price,
        uint256 timestamp,
        bool status
    );

    event RiceTransactionRecorded(
        uint256 indexed riceTxId,
        string indexed fromParty,
        string indexed toParty,
        string riceType,
        uint256 quantity,
        uint256 price,
        uint256 timestamp,
        bool status
    );

    // --- Update Events ---
    event InitialPaddyUpdated(
        uint256 indexed recordId,
        string userId,
        string paddyType,
        uint256 quantity,
        uint256 date,
        bool status
    );

    event InitialRiceUpdated(
        uint256 indexed recordId,
        string userId,
        string riceType,
        uint256 quantity,
        uint256 date,
        bool status
    );

    // --- Save Initial Paddy Record ---
    function saveInitialPaddyRecord(
        string calldata userId,
        string calldata paddyType,
        uint256 quantity,
        uint256 date,
        bool status
    ) external returns (uint256) {
        uint256 recordId = nextPaddyRecordId;
        if (recordId == 0) {
            recordId = 1;
            nextPaddyRecordId = 2;
        } else {
            nextPaddyRecordId = recordId + 1;
        }

        paddyRecordUserId[recordId] = userId;
        paddyRecordPaddyType[recordId] = paddyType;
        paddyRecordQuantity[recordId] = quantity;
        paddyRecordDate[recordId] = date;
        paddyRecordStatus[recordId] = status;

        emit InitialPaddyRecorded(
            recordId,
            userId,
            paddyType,
            quantity,
            date,
            status
        );

        return recordId;
    }

    // --- Save Initial Rice Record ---
    function saveInitialRiceRecord(
        string calldata userId,
        string calldata riceType,
        uint256 quantity,
        uint256 date,
        bool status
    ) external returns (uint256) {
        uint256 recordId = nextInitialRiceRecordId;
        if (recordId == 0) {
            recordId = 1;
            nextInitialRiceRecordId = 2;
        } else {
            nextInitialRiceRecordId = recordId + 1;
        }

        initialRiceRecordUserId[recordId] = userId;
        initialRiceRecordRiceType[recordId] = riceType;
        initialRiceRecordQuantity[recordId] = quantity;
        initialRiceRecordDate[recordId] = date;
        initialRiceRecordStatus[recordId] = status;

        emit InitialRiceRecorded(
            recordId,
            userId,
            riceType,
            quantity,
            date,
            status
        );

        return recordId;
    }

    // (bulk-save removed)

    // --- Transaction recording ---
    function recordTransaction(
        string calldata fromParty,
        string calldata toParty,
        string calldata productType,
        uint256 quantity,
        uint256 price,
        bool status
    ) external returns (uint256) {
        uint256 txId = nextTxId;
        // initialize nextTxId if zero (start at 1)
        if (txId == 0) {
            txId = 1;
            nextTxId = 2;
        } else {
            nextTxId = txId + 1;
        }

        txFromParty[txId] = fromParty;
        txToParty[txId] = toParty;
        txProductType[txId] = productType;
        txQuantity[txId] = quantity;
        txPrice[txId] = price;
        txTimestamp[txId] = block.timestamp;
        txStatus[txId] = status;

        sentTxs[fromParty].push(txId);
        receivedTxs[toParty].push(txId);

        emit TransactionRecorded(txId, fromParty, toParty, productType, quantity, price, block.timestamp, status);

        return txId;
    }

    // --- Rice Transaction recording ---
    function recordRiceTransaction(
        string calldata fromParty,
        string calldata toParty,
        string calldata riceType,
        uint256 quantity,
        uint256 price,
        bool status
    ) external returns (uint256) {
        uint256 riceTxId = nextRiceTxId;
        // initialize nextRiceTxId if zero (start at 1)
        if (riceTxId == 0) {
            riceTxId = 1;
            nextRiceTxId = 2;
        } else {
            nextRiceTxId = riceTxId + 1;
        }

        riceTxFromParty[riceTxId] = fromParty;
        riceTxToParty[riceTxId] = toParty;
        riceTxRiceType[riceTxId] = riceType;
        riceTxQuantity[riceTxId] = quantity;
        riceTxPrice[riceTxId] = price;
        riceTxTimestamp[riceTxId] = block.timestamp;
        riceTxStatus[riceTxId] = status;

        sentRiceTxs[fromParty].push(riceTxId);
        receivedRiceTxs[toParty].push(riceTxId);

        emit RiceTransactionRecorded(riceTxId, fromParty, toParty, riceType, quantity, price, block.timestamp, status);

        return riceTxId;
    }

    // --- Record Damage ---
    function recordDamage(
        string calldata userId,
        string calldata paddyType,
        uint256 quantity,
        uint256 damageDate,
        string calldata reason
    ) external returns (uint256) {
        uint256 damageId = nextDamageId;
        if (damageId == 0) {
            damageId = 1;
            nextDamageId = 2;
        } else {
            nextDamageId = damageId + 1;
        }

        damageUserId[damageId] = userId;
        damagePaddyType[damageId] = paddyType;
        damageQuantity[damageId] = quantity;
        damageDateTimestamp[damageId] = damageDate;
        damageReason[damageId] = reason;

        emit DamageRecorded(
            damageId,
            userId,
            paddyType,
            quantity,
            damageDate,
            reason
        );

        return damageId;
    }

    // --- Record Milling ---
    function recordMilling(
        uint256 inputPaddy,
        uint256 outputRice,
        uint256 dateTime,
        string calldata paddyType,
        uint256 dryingDuration,
        bool status
    ) external returns (uint256) {
        uint256 millingId = nextMillingRecordId;
        if (millingId == 0) {
            millingId = 1;
            nextMillingRecordId = 2;
        } else {
            nextMillingRecordId = millingId + 1;
        }

        millingInputPaddy[millingId] = inputPaddy;
        millingOutputRice[millingId] = outputRice;
        millingDateTime[millingId] = dateTime;
        millingPaddyType[millingId] = paddyType;
        millingDryingDuration[millingId] = dryingDuration;
        millingStatus[millingId] = status;

        emit MillingRecorded(
            millingId,
            inputPaddy,
            outputRice,
            dateTime,
            paddyType,
            dryingDuration,
            status
        );

        return millingId;
    }

    // --- Record Rice Damage ---
    function recordRiceDamage(
        string calldata userId,
        string calldata riceType,
        uint256 quantity,
        uint256 damageDate,
        string calldata reason
    ) external returns (uint256) {
        uint256 riceDamageId = nextRiceDamageId;
        if (riceDamageId == 0) {
            riceDamageId = 1;
            nextRiceDamageId = 2;
        } else {
            nextRiceDamageId = riceDamageId + 1;
        }

        riceDamageUserId[riceDamageId] = userId;
        riceDamageType[riceDamageId] = riceType;
        riceDamageQuantity[riceDamageId] = quantity;
        riceDamageDateTimestamp[riceDamageId] = damageDate;
        riceDamageReason[riceDamageId] = reason;

        emit RiceDamageRecorded(
            riceDamageId,
            userId,
            riceType,
            quantity,
            damageDate,
            reason
        );

        return riceDamageId;
    }

    // --- Getters ---
    function getInitialPaddyRecord(uint256 recordId) external view returns (
        string memory userId,
        string memory paddyType,
        uint256 quantity,
        uint256 date,
        bool status
    ) {
        require(recordId > 0 && recordId < nextPaddyRecordId, "Invalid record ID");
        return (
            paddyRecordUserId[recordId],
            paddyRecordPaddyType[recordId],
            paddyRecordQuantity[recordId],
            paddyRecordDate[recordId],
            paddyRecordStatus[recordId]
        );
    }

    function getInitialRiceRecord(uint256 recordId) external view returns (
        string memory userId,
        string memory riceType,
        uint256 quantity,
        uint256 date,
        bool status
    ) {
        require(recordId > 0 && recordId < nextInitialRiceRecordId, "Invalid record ID");
        return (
            initialRiceRecordUserId[recordId],
            initialRiceRecordRiceType[recordId],
            initialRiceRecordQuantity[recordId],
            initialRiceRecordDate[recordId],
            initialRiceRecordStatus[recordId]
        );
    }

    function getTransaction(uint256 txId) external view returns (
        string memory fromParty,
        string memory toParty,
        string memory productType,
        uint256 quantity,
        uint256 price,
        uint256 timestamp,
        bool status
    ) {
        require(txId > 0 && txId < nextTxId, "Invalid tx ID");
        return (
            txFromParty[txId],
            txToParty[txId],
            txProductType[txId],
            txQuantity[txId],
            txPrice[txId],
            txTimestamp[txId],
            txStatus[txId]
        );
    }

    function getRiceTransaction(uint256 riceTxId) external view returns (
        string memory fromParty,
        string memory toParty,
        string memory riceType,
        uint256 quantity,
        uint256 price,
        uint256 timestamp,
        bool status
    ) {
        require(riceTxId > 0 && riceTxId < nextRiceTxId, "Invalid rice tx ID");
        return (
            riceTxFromParty[riceTxId],
            riceTxToParty[riceTxId],
            riceTxRiceType[riceTxId],
            riceTxQuantity[riceTxId],
            riceTxPrice[riceTxId],
            riceTxTimestamp[riceTxId],
            riceTxStatus[riceTxId]
        );
    }

    function getDamage(uint256 damageId) external view returns (
        string memory userId,
        string memory paddyType,
        uint256 quantity,
        uint256 damageDate,
        string memory reason
    ) {
        require(damageId > 0 && damageId < nextDamageId, "Invalid damage ID");
        return (
            damageUserId[damageId],
            damagePaddyType[damageId],
            damageQuantity[damageId],
            damageDateTimestamp[damageId],
            damageReason[damageId]
        );
    }

    function getMilling(uint256 millingId) external view returns (
        uint256 inputPaddy,
        uint256 outputRice,
        uint256 dateTime,
        string memory paddyType,
        uint256 dryingDuration,
        bool status
    ) {
        require(millingId > 0 && millingId < nextMillingRecordId, "Invalid milling ID");
        return (
            millingInputPaddy[millingId],
            millingOutputRice[millingId],
            millingDateTime[millingId],
            millingPaddyType[millingId],
            millingDryingDuration[millingId],
            millingStatus[millingId]
        );
    }

    function getRiceDamage(uint256 riceDamageId) external view returns (
        string memory userId,
        string memory riceType,
        uint256 quantity,
        uint256 damageDate,
        string memory reason
    ) {
        require(riceDamageId > 0 && riceDamageId < nextRiceDamageId, "Invalid rice damage ID");
        return (
            riceDamageUserId[riceDamageId],
            riceDamageType[riceDamageId],
            riceDamageQuantity[riceDamageId],
            riceDamageDateTimestamp[riceDamageId],
            riceDamageReason[riceDamageId]
        );
    }
}

