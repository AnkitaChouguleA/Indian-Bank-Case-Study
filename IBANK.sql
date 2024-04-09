USE IBANK

--1.	Account ID, Branch ID, Product ID and Region ID fields should be UNIQUE
-- check whether PK is present on above columns so bydefault it is unique
sp_help 'AMASTER'
sp_help 'BRMASTER'
sp_help 'PMASTER'
sp_help 'RMASTER'

--2.	Date of Transaction (DOT) and Date of Opening (DOO) should be the  current date
-- Alter the TMASTER table to set default value for DOT column
begin tran
ALTER TABLE ibank.dbo.TMASTER
ALTER COLUMN DOT DATETIME DEFAULT GETDATE();

-- Alter the AMASTER table to set default value for DOO column
ALTER TABLE ibank.dbo.AMASTER
ALTER COLUMN DOO DATETIME DEFAULT GETDATE();
--giving error

--3.	A Cheque which is more than six months old should not be accepted
-- Define CHECK constraint for cheque date
begin tran
ALTER TABLE TMASTER
ADD CONSTRAINT CHK_CHEQUE_DATE CHECK (CHQDATE >= DATEADD(MONTH, -6, GETDATE()));
--cannot add because already have data in the table. above is code before adding data to table.

--4. No Transactions should be allowed on Accounts marked “Inoperative/closed”
CREATE or alter trigger Check_StatusAndUpdateBalance  
on Tmaster  
FOR Insert,Update,Delete  
as  
begin  
  
	declare @acid int  
	declare @type char(3)  
	declare @amt money  
	declare @status char(1)  
	declare @bal money  
  
	--Get Customer info  
	select @acid = ACID, @type = TXNTYPE , @amt = TXNAMT  from inserted  
  
	--Find out the status  
	select @status = status from AMASTER where acid = @acid  
  
	--Open  
	if (@status = 'O')  
		begin  
  
		If (@type = 'CD')  
			begin  
				update AMASTER set cbal = cbal + @amt where acid = @acid  
			end  
		else  
			begin  
				--check the balance  
				select @bal = cbal from AMASTER where acid = @acid  
      
				if (@amt <= @bal)   
					update AMASTER set cbal = cbal - @amt where acid = @acid  
				else  

				begin  
					print 'Insufficient funds in your account..'  
					print 'Txn is declined'  
					rollback  
				end  
			end  
		end  
	else  
		begin  
			print 'Your account is de-activated. Please call Customer Care'  
			print 'Txn is declined'  
			rollback  
		end  
end  

insert into TMASTER values (GETDATE(), 105, 'BR1','CD',null,null,20000,1)

--5.	When a Transaction is altered, the difference between the old amount and the new amount 
--		cannot be more than 10%, if the transaction has been effected by the teller
use IBANK

create or alter trigger Trg_Tran_Audit
on tmaster 
after update
as
begin
	declare @oldAmt money
	declare @newAmt money
	declare @teller char(1)

	select @oldAmt = d.TXNAMT, @newAmt = i.TXNAMT, @teller = u.DESGN 
	from inserted i 
	inner join deleted d
	on i.TNO = d.TNO
	inner join UMASTER u
	on u.UID = i.UID

	if @teller = 'T' and ABS(@newAmt - @oldAmt) > (0.1 * @oldAmt)
	begin 
		RAISERROR ('Transaction alteration exceeds 10% limit for teller-affected transactions.', 16, 1);
        ROLLBACK TRANSACTION;
	end
end
go

print 0.1 * 5000

update TMASTER
set TXNAMT = 2500
where TNO = 4

--6.	More than three Cash Withdrawal transactions in a single account on the same day should not be allowed
create or alter trigger TranLimit
on Tmaster
for insert,update
as
begin
	declare @acid int
	declare @TxnDate Date
	declare @WithdrawlsToday int

	select @acid = i.ACID, @TxnDate = CONVERT(date, DOT)
	from inserted i

	-- Count the number of cash withdrawal transactions for the account on the same day
	select @WithdrawlsToday = count(*)
	from TMASTER
	where ACID = @acid
	and TXNTYPE = 'CW'
	and convert(date,DOT) = @TxnDate

	if @WithdrawlsToday > 3
	BEGIN
        Raiserror('More than three cash withdrawal transactions in a single account on the same day are not allowed.',16,1)
        ROLLBACK TRANSACTION;
    END
end
go

-- Begin a transaction
BEGIN TRANSACTION;

-- Perform the INSERT statements
INSERT INTO TMASTER (DOT, ACID, BRID, TXNTYPE, TXNAMT, UID)
VALUES
('2024-03-01', 101, 'BR1', 'CW', 2000.00, 1),
('2024-03-01', 101, 'BR1', 'CW', 2000.00, 1),
('2024-03-01', 101, 'BR1', 'CW', 2000.00, 1),
('2024-03-01', 101, 'BR1', 'CW', 2000.00, 1);

select * from TMASTER where ACID = 101  and TXNTYPE = 'CW' and TXNAMT = 2000

-- 7.	More than three Cash Deposit  transactions in a single account on the same month should not be allowed
use IBANK
CREATE OR ALTER TRIGGER Trg_CashDepositLimit
ON TMASTER
AFTER INSERT, UPDATE
AS
BEGIN
    DECLARE @ACID INT;
    DECLARE @TxnMonth DATE;
    DECLARE @DepositsThisMonth INT;

    -- Get the inserted account ID and transaction month
    SELECT @ACID = ACID, @TxnMonth = DATEFROMPARTS(YEAR(DOT), MONTH(DOT), 1)
    FROM inserted;

    -- Count the number of cash deposit transactions for the account in the same month
    SELECT @DepositsThisMonth = COUNT(*)
    FROM TMASTER
    WHERE ACID = @ACID
    AND TXNTYPE = 'CD'
    AND DATEFROMPARTS(YEAR(DOT), MONTH(DOT), 1) = @TxnMonth;

    -- Check if the number of deposits exceeds three
    IF @DepositsThisMonth > 3
    BEGIN
        RAISERROR ('More than three cash deposit transactions in a single account on the same month are not allowed.', 16, 1);
        ROLLBACK TRANSACTION;
    END
END;

-- Insert cash deposit transactions
INSERT INTO TMASTER (DOT, ACID, BRID, TXNTYPE, TXNAMT, UID)
VALUES
('2024-03-01', 101, 'BR1', 'CD', 2000.00, 1),
('2024-03-10', 101, 'BR1', 'CD', 1500.00, 1),
('2024-03-15', 101, 'BR1', 'CD', 1800.00, 1),
('2024-03-20', 101, 'BR1', 'CD', 2500.00, 1);  -- This will exceed the limit.This insert should fail due to the trigger

-- 8.	Cheque Number and Cheque Date columns should not be ‘NULL’, if the Transaction type is ‘Cheque Deposit’
CREATE OR ALTER TRIGGER CheckChequeDetails
ON TMASTER
AFTER INSERT, UPDATE
AS
BEGIN
    -- Check if there are any rows being inserted or updated with Transaction type 'Cheque Deposit'
    IF EXISTS (SELECT 1 FROM inserted WHERE TXNTYPE = 'CQD')
    BEGIN
        -- Check if any of the inserted or updated rows have NULL values for Cheque Number or Cheque Date
        IF EXISTS (SELECT 1 FROM inserted WHERE TXNTYPE = 'CQD' AND (CHQNO IS NULL OR CHQDATE IS NULL))
        BEGIN
            -- Rollback the transaction and raise an error message
            ROLLBACK TRANSACTION;
            RAISERROR ('Cheque Number and Cheque Date cannot be NULL for Cheque Deposit transactions.', 16, 1);
        END
    END
END;

-- Insert transactions with 'Cheque Deposit' type and NULL values for Cheque Number and Cheque Date
INSERT INTO TMASTER (DOT, ACID, BRID, TXNTYPE, CHQNO, CHQDATE, TXNAMT, UID)
VALUES
('2024-03-01', 101, 'BR1', 'CQD', NULL, NULL, 5000.00, 1),
('2024-03-05', 102, 'BR2', 'CQD', NULL, NULL, 7000.00, 2);


select * from TMASTER where TXNTYPE = 'CQD'

-- 9.	A product should not be removed, if there are accounts attached to it (Similar checks are required wherever appropriate)
CREATE OR ALTER TRIGGER PreventProductRemoval
ON PMASTER
INSTEAD OF DELETE
AS
BEGIN
    -- Check if there are any accounts attached to the product being deleted
    IF EXISTS (
        SELECT 1
        FROM AMASTER
        WHERE PID IN (SELECT PID FROM deleted)
    )
    BEGIN
        -- Raise an error message indicating that the product cannot be removed
        print 'A product cannot be removed if there are accounts attached to it.'
    END
    ELSE
    BEGIN
        -- If no accounts are attached, proceed with the deletion
        DELETE FROM PMASTER WHERE PID IN (SELECT PID FROM deleted);
    END
END;

DELETE FROM PMASTER WHERE PID = 'SB';--it will give error

-- 10.	Transaction Amount should not be negative
CREATE OR ALTER TRIGGER PreventNegativeTransactionAmount
ON TMASTER
FOR INSERT, UPDATE
AS
BEGIN
    -- Check if any transaction amount is negative
    IF EXISTS (SELECT 1 FROM inserted WHERE TXNAMT < 0)
    BEGIN
        -- Rollback the transaction and raise an error
        print'Transaction Amount cannot be negative.'
        ROLLBACK TRANSACTION;
        RETURN;
    END
END;
-- Attempt to update a transaction amount to a negative value
UPDATE TMASTER SET TXNAMT = -500 WHERE TNO = 1;

-- 11.	Transaction Type should only be ‘CW’ or ‘CD’ or ‘CQD’
ALTER TABLE TMASTER
ADD CONSTRAINT CHK_TransactionType
CHECK (TXNTYPE IN ('CW', 'CD', 'CQD'));

-- Update an existing row to an invalid transaction type (CD)
UPDATE TMASTER
SET TXNTYPE = 'CDW'
WHERE TNO = 123; -- Replace with the appropriate transaction number

--12.	An account should not be closed, if its related Cheques are in transit 
--		(i.e. if the Cleared and Uncleared balances are not equal)
CREATE OR ALTER TRIGGER PreventCloseWithChequesInTransit
ON AMASTER
INSTEAD OF UPDATE
AS
BEGIN
    IF EXISTS (
        SELECT 1
        FROM INSERTED i
        INNER JOIN AMASTER a ON i.ACID = a.ACID
        WHERE i.STATUS = 'C' AND ABS(a.CBAL - a.UBAL) > 0
    )
    BEGIN
        PRINT'Cannot close account due to difference between Cleared and Uncleared balances.'
        ROLLBACK TRANSACTION;
    END
    ELSE
    BEGIN
        UPDATE AMASTER
        SET STATUS = i.STATUS
        FROM INSERTED i
        WHERE AMASTER.ACID = i.ACID;
    END
END;

-- Attempt to close an account with a difference between Cleared and Uncleared balances
UPDATE AMASTER
SET STATUS = 'C'
WHERE ACID = 123; -- Replace 123 with the ID of the account you want to test
-- After executing the update, check the messages or logs to see if the trigger executed successfully

--13.	Uncleared balance should not be less than Cleared balance
ALTER TABLE AMASTER
ADD CONSTRAINT CHK_UnclearedBalance
CHECK (UBAL >= CBAL);

sp_help Amaster

select * from AMASTER
WHERE UBAL < CBAL

-- Disable the trigger
DISABLE TRIGGER PreventCloseWithChequesInTransit ON AMASTER;

-- Update UBAL for rows where UBAL < CBAL
UPDATE AMASTER
SET UBAL = CBAL + 10
WHERE UBAL < CBAL;

-- Re-enable the trigger
ENABLE TRIGGER PreventCloseWithChequesInTransit ON AMASTER;

--test constraint
INSERT INTO AMASTER 
VALUES (168, 'Ankita Chougule ', 'Hebbal Kasaba Nool', 'BR1', 'SB', GETDATE(), 1000.00, 900.00, 'O');

-- 14.	Minimum balance for Savings Bank should be Rs. 1,000/=
ALTER TABLE AMASTER
ADD CONSTRAINT CHK_MinimumBalance_SavingsBank
CHECK (
    (PID = 'SB' AND CBAL >= 1000.00) OR  -- Checking if the account type is Savings Bank and CBAL is greater than or equal to 1000
    (PID != 'SB') -- All other account types are not subject to this constraint
);

select * from AMASTER
where PID = 'SB' and CBAL < 1000

begin tran

update AMASTER
set UBAL = 10000
where acid = 135

update AMASTER
set CBAL = 1500
where acid = 135

commit tran
--data not getting updated. And because there is already data which violates above constraint cannot create above constraint on it.

--15.	When there is an insert/update in the transaction amount for an account, Balance (Clear & Unclear) 
--		in the ‘Account Master’ should be updated implicitly. 
CREATE OR ALTER TRIGGER UpdateAccountBalance
ON TMASTER
AFTER INSERT, UPDATE
AS
BEGIN
    --SET NOCOUNT ON;
    
    BEGIN TRY
        BEGIN TRANSACTION;

        -- Update Clear and Unclear balance for each affected account
        UPDATE AMASTER
        SET CBAL = CASE 
                        WHEN i.TXNTYPE = 'CD' THEN CBAL + i.TXNAMT
                        WHEN i.TXNTYPE = 'CW' THEN CBAL - i.TXNAMT
                        WHEN i.TXNTYPE = 'CQD' THEN CBAL + i.TXNAMT  -- Handling cheque deposits
                        ELSE CBAL
                    END,
            UBAL = CASE 
                        WHEN i.TXNTYPE = 'CD' THEN UBAL + i.TXNAMT
                        WHEN i.TXNTYPE = 'CW' THEN UBAL - i.TXNAMT
                        WHEN i.TXNTYPE = 'CQD' THEN UBAL + i.TXNAMT  -- Handling cheque deposits
                        ELSE UBAL
                    END
        FROM AMASTER AS a
        JOIN inserted AS i ON a.ACID = i.ACID;

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        -- Rollback the transaction if an error occurs
        ROLLBACK TRANSACTION;

        -- Optionally, log the error or raise a custom error message
        THROW;
    END CATCH;
END;

INSERT INTO TMASTER (DOT, ACID, BRID, TXNTYPE, TXNAMT, UID)
VALUES ('2024-04-05', 101, 'BR1', 'CD', 10000.00, 1);

INSERT INTO TMASTER (DOT, ACID, BRID, TXNTYPE, TXNAMT, UID)
VALUES ('2024-04-05', 101, 'BR1', 'CW', 10000.00, 1);

SELECT name, is_disabled
FROM sys.triggers
WHERE name = 'UpdateAccountBalance';

SELECT * FROM sys.dm_exec_trigger_stats;
--trigger not working properly. 

-- 16.	If there is no minimum balance in the account, withdrawal should be prohibited and an appropriate message should be displayed. 
CREATE OR ALTER TRIGGER ProhibitWithdrawal
ON TMASTER
AFTER INSERT, UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @AccountID INT, @TransactionType CHAR(2), @TransactionAmount MONEY, @ClearedBalance MONEY;

    -- Get the Account ID, Transaction Type, Transaction Amount, and Cleared Balance from the inserted rows
    SELECT @AccountID = i.ACID, @TransactionType = i.TXNTYPE, @TransactionAmount = i.TXNAMT, @ClearedBalance = a.CBAL
    FROM inserted i
    INNER JOIN AMASTER a ON i.ACID = a.ACID;

    -- Check if the Transaction Type is a withdrawal and the Cleared Balance is less than the minimum balance
    IF @TransactionType = 'CW' AND @ClearedBalance - @TransactionAmount < 1000  -- Assuming minimum balance is Rs. 1,000
    BEGIN
        -- Rollback the transaction and display a message
        RAISERROR ('Withdrawal prohibited: Account does not have the minimum required balance.', 16, 1);
        ROLLBACK TRANSACTION;
    END
END;

select * from AMASTER where cbal > 1000 and cbal < 2000

INSERT INTO TMASTER (DOT, ACID, BRID, TXNTYPE, TXNAMT, UID)
VALUES ('2024-04-13', 154, 'BR1', 'CW', 1000.00, 1);

-- 17.	If the transaction amount is greater than Rs. 50,000/=, the same should be inserted into the ‘High Value Transaction’ table.
create or alter trigger High_Volumn_Txns  
on Tmaster  
Instead of Insert,Update,Delete  
as  
begin  
  
declare @dot datetime  
declare @acid int  
declare @brid char(3)  
declare @txntype char(3)  
declare @chqno int  
declare @chqdate datetime  
declare @txnamt money  
declare @uid int  
  
--Get the customer info  
select @dot = [DOT],@acid= [ACID],@brid= [BRID],   
 @txntype = [TXNTYPE], @chqno = [CHQNO], @chqdate = [CHQDATE], @txnamt = [TXNAMT],@uid=[UID]  
from inserted  
  
--Condition  
if (@txnamt > 50000)  
 insert into HTM Values (@dot, @acid,@brid,@txntype , @chqno , @chqdate , @txnamt ,@uid)  
else  
 insert into Tmaster Values (@dot, @acid,@brid,@txntype , @chqno , @chqdate , @txnamt ,@uid)  
  
end  

insert into TMASTER values (GETDATE(), 105, 'BR1','CD',null,null,200000,1)

select * from TMASTER

select * from HTM

--18.	Total no. of transactions per month to be less than 5. If it exceeds Rs. 50/= to be debited as penalty. 
CREATE OR ALTER TRIGGER LimitTransactionCount
ON TMASTER
AFTER INSERT, UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Month INT, @Year INT, @TransactionCount INT;

    SELECT @Month = MONTH(DOT), @Year = YEAR(DOT) FROM inserted;

    -- Count the total number of transactions for the month
    SELECT @TransactionCount = COUNT(*)
    FROM TMASTER
    WHERE MONTH(DOT) = @Month AND YEAR(DOT) = @Year;

    IF @TransactionCount > 5
    BEGIN
        -- Apply penalty of Rs. 50
        UPDATE AMASTER
        SET CBAL = CBAL - 50, UBAL = UBAL - 50
        WHERE ACID IN (SELECT ACID FROM inserted);
    END
END;

-- Insert transactions for a specific month (ensure it exceeds 5)
INSERT INTO TMASTER (DOT, ACID, BRID, TXNTYPE, TXNAMT, UID)
VALUES
    ('2024-04-01', 101, 'BR1', 'CW', 2000.00, 1),
    ('2024-04-05', 101, 'BR1', 'CW', 2000.00, 1),
    ('2024-04-10', 101, 'BR1', 'CW', 2000.00, 1),
    ('2024-04-15', 101, 'BR1', 'CW', 2000.00, 1),
    ('2024-04-20', 101, 'BR1', 'CW', 2000.00, 1),
    ('2024-04-25', 101, 'BR1', 'CW', 2000.00, 1),
    ('2024-04-30', 101, 'BR1', 'CW', 2000.00, 1);  -- Total transactions for April: 7

-- After inserting the transactions, verify the balances in AMASTER table to ensure the penalty is applied correctly
SELECT * FROM AMASTER WHERE ACID = 101;

--data in AMASTER NOT GETTING Updated

--19.Total cash withdrawals allowed in a day is Rs.50,000/-. When this is exceeded, a charge of 1% on extra amountis to be debited as penalty.  
CREATE OR ALTER TRIGGER LimitDailyWithdrawal
ON TMASTER
AFTER INSERT, UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Variables to store the total withdrawal amount and the excess amount
    DECLARE @TotalWithdrawal DECIMAL(18, 2);
    DECLARE @ExcessAmount DECIMAL(18, 2);
    DECLARE @Penalty DECIMAL(18, 2);
    DECLARE @MaxDailyWithdrawal DECIMAL(18, 2) = 50000.00; -- Maximum daily withdrawal limit
    
    -- Calculate the total withdrawal amount for the affected account on the current day
    SELECT @TotalWithdrawal = SUM(TXNAMT)
    FROM TMASTER
    WHERE ACID IN (SELECT ACID FROM inserted)
    AND DOT = CONVERT(DATE, GETDATE())
    AND TXNTYPE = 'CW'; -- Consider only cash withdrawal transactions
    
    -- Check if the total withdrawal exceeds the daily limit
    IF @TotalWithdrawal > @MaxDailyWithdrawal
    BEGIN
        -- Calculate the excess amount
        SET @ExcessAmount = @TotalWithdrawal - @MaxDailyWithdrawal;
        -- Calculate the penalty (1% of the excess amount)
        SET @Penalty = 0.01 * @ExcessAmount;

        -- Apply penalty for each affected account
        UPDATE AMASTER
        SET CBAL = CBAL - @Penalty, -- Deduct penalty from the current balance
            UBAL = UBAL - @Penalty
        WHERE ACID IN (SELECT ACID FROM inserted);
        
        -- Optionally, you can log the penalty or display a message
        
        PRINT 'Exceeded daily withdrawal limit. Penalty applied for excess amount: ' + CAST(@Penalty AS VARCHAR(20));
    END
END;


-- Insert cash withdrawal transactions exceeding the daily limit for a specific day
INSERT INTO TMASTER (DOT, ACID, BRID, TXNTYPE, TXNAMT, UID)
VALUES
    ('2024-04-04', 102, 'BR1', 'CW', 30000.00, 1),  -- First withdrawal (Rs. 30,000)
    ('2024-04-04', 102, 'BR1', 'CW', 30000.00, 1),  -- Second withdrawal (Rs. 30,000)
    ('2024-04-04', 102, 'BR1', 'CW', 20000.00, 1);  -- Third withdrawal (Rs. 20,000)

-- After inserting the transactions, verify the balances in the AMASTER table to ensure the penalty is applied correctly
SELECT * FROM AMASTER WHERE ACID = 102;

-- amaster table data not getting updateds

--II. VIEW REQUIREMENTS
-- 1.	Only the Account Number, Name and Address from the ‘Account Master’
CREATE VIEW AccountDetails AS
SELECT ACID, NAME, ADDRESS
FROM AMASTER;

select * from AccountDetails

-- 2.	Account Number, Name, Date of last Transaction, total number of transactions in the Account
CREATE VIEW AccountTransactionSummary AS
SELECT A.ACID, A.NAME, MAX(DOT) AS LastTransactionDate, COUNT(*) AS TotalTransactions
FROM AMASTER A
INNER JOIN TMASTER T
ON A.ACID = T.ACID
GROUP BY A.ACID, A.NAME;

SELECT * FROM AccountTransactionSummary

-- 3.	Branch-wise, Product-wise, sum of Uncleared balance
CREATE OR ALTER VIEW BranchProductBalance AS
SELECT BRID, PID, SUM(UBAL) AS TotalUnclearedBalance
FROM AMASTER
GROUP BY BRID, PID

SELECT * FROM BranchProductBalance ORDER BY BRID, PID;

-- 4.	Customer-wise, number of accounts held   
CREATE VIEW CustomerAccountCount AS
SELECT NAME, COUNT(*) AS NumberOfAccounts
FROM AMASTER
GROUP BY NAME;

SELECT * FROM CustomerAccountCount

-- 5.	TransactionType-wise, Account-wise, sum of transaction amount for the current month
CREATE VIEW TransactionTypeAccountSummary AS
SELECT TXNTYPE, ACID, SUM(TXNAMT) AS TotalTransactionAmount
FROM TMASTER
WHERE MONTH(DOT) = MONTH(GETDATE()) AND YEAR(DOT) = YEAR(GETDATE())
GROUP BY TXNTYPE, ACID;

SELECT * FROM TransactionTypeAccountSummary

/*
CTRL + K + C = TO COMMENT
CTRL + K + U = TO UNCOMMENT
*/

--III. QUERY REQUIREMENTS 
--1.	List the transactions that have taken place in a given Branch during the  previous month
SELECT * FROM TMASTER
WHERE BRID = 'BR1'
AND MONTH(DOT) = MONTH(DATEADD(MONTH, -1, GETDATE()))
AND YEAR(DOT) = YEAR(DATEADD(MONTH, -1, GETDATE()));

--2.	Give the branch-wise total cash deposits that have taken place during the last 5 days
SELECT BRID, SUM(TXNAMT) AS TotalCashDeposits
FROM TMASTER
WHERE TXNTYPE = 'CD'
AND DOT >= DATEADD(DAY, -5, GETDATE())
GROUP BY BRID;

--3.	Give the branch-wise total cash withdrawals during the last month, where the total cash withdrawals are greater than Rs 1,00,000
SELECT BRID, SUM(TXNAMT) AS [Total Cash Withdrawals]
FROM TMASTER
WHERE TXNTYPE = 'CW'
AND DOT >= DATEADD(MONTH, -1, GETDATE())
GROUP BY BRID
HAVING SUM(TXNAMT) > 100000;

--4.	List the names of the account holders with corresponding branch names, in respect of the maximum and minimum Cleared balance 
SELECT a.NAME, a.ADDRESS, a.BRID, a.CBAL
FROM AMASTER a
INNER JOIN (
    SELECT MAX(CBAL) AS MaxBalance, MIN(CBAL) AS MinBalance
    FROM AMASTER
) b ON a.CBAL IN (b.MaxBalance, b.MinBalance);

--5.	List the names of the account holders with corresponding branch names, in respect of the second-highest maximum and minimum Cleared balance
SELECT a.NAME, a.ADDRESS, a.BRID, a.CBAL
FROM AMASTER a
INNER JOIN (
    SELECT CBAL, ROW_NUMBER() OVER (ORDER BY CBAL DESC) AS rn -- this gives second max cbal
    FROM AMASTER
) b ON a.CBAL = b.CBAL
WHERE b.rn = 2
UNION ALL
SELECT a.NAME, a.ADDRESS, a.BRID, a.CBAL
FROM AMASTER a
INNER JOIN (
    SELECT CBAL, ROW_NUMBER() OVER (ORDER BY CBAL ASC) AS rn -- this gives second min cbal
    FROM AMASTER
) b ON a.CBAL = b.CBAL
WHERE b.rn = 2;

--6.	List the name of the account holder who has the second-highest cleared balance in the branch having the account with the maximum cleared balance.
SELECT a.NAME, a.ADDRESS, a.BRID, a.CBAL
FROM AMASTER a
INNER JOIN (
    SELECT BRID, MAX(CBAL) AS MaxBalance
    FROM AMASTER
    GROUP BY BRID
) b ON a.BRID = b.BRID
WHERE a.CBAL = (
    SELECT MAX(CBAL) AS SecondMaxBalance
    FROM AMASTER
    WHERE BRID = b.BRID
    AND CBAL < b.MaxBalance
);

--or
WITH RankedAccounts AS (
    SELECT NAME, ADDRESS, BRID, CBAL,
    ROW_NUMBER() OVER (PARTITION BY BRID ORDER BY CBAL DESC) AS Rank
    FROM AMASTER
)
SELECT NAME, ADDRESS, BRID, CBAL
FROM RankedAccounts
WHERE Rank = 2;

--7.	Give the TransactionType-wise, branch-wise, total amount for the day
SELECT TXNTYPE, BRID, SUM(TXNAMT) AS TotalAmount
FROM TMASTER
WHERE CONVERT(DATE, DOT) = CONVERT(DATE, GETDATE()) -- Filter transactions for the current day
GROUP BY TXNTYPE,BRID;

--8.	Give the names of the account holders who have not put thru not even a single Cash deposit transaction during the last 15 days
SELECT DISTINCT AM.NAME, AM.ADDRESS, tm.TXNTYPE
FROM AMASTER AM
LEFT JOIN TMASTER TM 
ON AM.ACID = TM.ACID
WHERE TM.TXNTYPE <> 'CD'
      AND TM.DOT < DATEADD(DAY, -15, GETDATE());

--9.	List the product having the maximum number of accounts
select top 1 p.PID, COUNT(ACID) as [NUMBER OF ACCOUNTS] from PMASTER p
join AMASTER a
on p.PID = a.PID
group by p.PID
order by [NUMBER OF ACCOUNTS] desc

--10.	List the product having the maximum monthly, average number of transactions (consider the last 6 months data)
SELECT TOP 1 p.PID, p.PNAME, AVG(NumberOfTransactions) AS AvgTransactions
FROM (
    SELECT a.PID, YEAR(t.DOT) AS TransactionYear, MONTH(t.DOT) AS TransactionMonth, COUNT(*) AS NumberOfTransactions
    FROM TMASTER t
    INNER JOIN AMASTER a ON t.ACID = a.ACID
    WHERE t.DOT >= DATEADD(MONTH, -6, GETDATE()) -- Filter data for the last 6 months
    GROUP BY a.PID, YEAR(t.DOT), MONTH(t.DOT)
) AS MonthlyTransactions
INNER JOIN PMASTER p ON MonthlyTransactions.PID = p.PID
GROUP BY p.PID, p.PNAME
ORDER BY AvgTransactions DESC;

--11.	List the product showing an increasing trend in average number of transactions per month.
WITH MonthlyAvgTransactions 
AS (
    SELECT p.PID, p.PNAME,
		YEAR(t.DOT) AS TransactionYear,
        MONTH(t.DOT) AS TransactionMonth,
        COUNT(*) AS NumberOfTransactions,
        AVG(COUNT(*)) OVER (PARTITION BY p.PID, YEAR(t.DOT), MONTH(t.DOT)) AS AvgTransactions
    FROM TMASTER t
    INNER JOIN AMASTER a ON t.ACID = a.ACID
    INNER JOIN PMASTER p ON a.PID = p.PID
    GROUP BY p.PID, p.PNAME, YEAR(t.DOT), MONTH(t.DOT)
)
SELECT PID, PNAME
FROM (
		SELECT PID, PNAME, TransactionYear, TransactionMonth, AvgTransactions,
        LAG(AvgTransactions) OVER (PARTITION BY PID ORDER BY TransactionYear, TransactionMonth) AS PreviousAvgTransactions
		FROM MonthlyAvgTransactions
	) AS Trends
WHERE AvgTransactions > ISNULL(PreviousAvgTransactions, 0)
GROUP BY PID, PNAME;

--12.	List the names of the account holders and the number of transactions put thru by them, in a given day.
SELECT a.NAME, COUNT(t.TNO) AS NumTransactions
FROM AMASTER a
INNER JOIN TMASTER t ON a.ACID = t.ACID
WHERE CONVERT(DATE, t.DOT) between '2022-04-04' and '2024-04-04' -- Replace '2024-04-04' with the desired date
GROUP BY a.NAME;

--13.	List the account holder’s name, account number and sum amount for customers who have made more than one cash withdrawal 
--  transaction in the same day (Consider the transactions in the last 20 days for this query)
SELECT a.NAME, a.ACID, SUM(t.TXNAMT) AS TotalAmount
FROM AMASTER a
JOIN TMASTER t ON a.ACID = t.ACID
WHERE t.TXNTYPE = 'CW'
AND t.DOT >= DATEADD(DAY, -20, GETDATE()) -- Consider transactions in the last 20 days
GROUP BY a.NAME, a.ACID, CONVERT(DATE, t.DOT)
HAVING COUNT(*) > 1; -- Having more than one cash withdrawal transaction on the same day

--14.	List the account holder’s name, account number and amount for customers who have made at least one transaction 
--    in each transaction type in the last 10 days
SELECT a.NAME, a.ACID, SUM(t.TXNAMT) AS TotalAmount
FROM AMASTER a
JOIN TMASTER t ON a.ACID = t.ACID
WHERE t.DOT >= DATEADD(DAY, -10, GETDATE()) -- Consider transactions in the last 10 days
GROUP BY a.NAME, a.ACID
HAVING COUNT(DISTINCT t.TXNTYPE) = (SELECT COUNT(DISTINCT TXNTYPE) FROM TMASTER WHERE DOT >= DATEADD(DAY, -10, GETDATE()));

--15.	List the number of transactions that have been authorized by the Manager so far today
SELECT COUNT(*) AS AuthorizedTransactions
FROM TMASTER
WHERE UID IN (SELECT UID FROM UMASTER WHERE  DESGN = 'Manager')
AND CONVERT(DATE, DOT) = CONVERT(DATE, GETDATE());

--16.	Considering all transactions which took place in the last 3 days, give the region-wise, branch-wise breakup of number 
--   of transactions only for those regions where the total number of transactions exceeds 100.
SELECT r.RNAME, b.BRID, COUNT(*) AS TransactionCount
FROM TMASTER t
JOIN AMASTER a ON t.ACID = a.ACID
JOIN BRMASTER b ON a.BRID = b.BRID
JOIN RMASTER r ON b.RID = r.RID
WHERE t.DOT >= DATEADD(DAY, -3, GETDATE())
GROUP BY r.RNAME, b.BRID
HAVING COUNT(*) > 100;

--17.	List the names of the clients who have accounts in all the products
SELECT a.NAME FROM AMASTER a
GROUP BY a.NAME
HAVING COUNT(DISTINCT a.PID) = (SELECT COUNT(DISTINCT PID) FROM PMASTER);

--18.	List the accounts that are likely to become “Inoperative” next month
SELECT ACID, NAME, ADDRESS, BRID, PID
FROM AMASTER
GROUP BY ACID, NAME, ADDRESS, BRID, PID
HAVING DATEDIFF(MONTH, MAX(DOO), GETDATE()) >= 12;

--19.	List the user who has entered the maximum number of transactions today
SELECT TOP 1 UID, COUNT(*) AS TransactionCount
FROM TMASTER
WHERE CONVERT(DATE, DOT) = CONVERT(DATE, GETDATE()) -- Filter transactions for today
GROUP BY UID
ORDER BY TransactionCount DESC;

--20.	Given a branch, list the heaviest day in terms of number of transactions/value of Cash Deposits during the last one month
--For the number of transactions
SELECT TOP 1 CONVERT(DATE, DOT) AS TransactionDate, COUNT(*) AS TransactionCount
FROM TMASTER
WHERE BRID = 'YourBranchID'
  AND DOT >= DATEADD(MONTH, -1, GETDATE()) -- Filter transactions for the last one month
GROUP BY CONVERT(DATE, DOT)
ORDER BY TransactionCount DESC;

--For the value of cash deposits:
SELECT TOP 1 CONVERT(DATE, DOT) AS TransactionDate, SUM(TXNAMT) AS TotalCashDeposits
FROM TMASTER
WHERE BRID = 'BR1'
  AND TXNTYPE = 'CD' -- Filter cash deposit transactions
  AND DOT >= DATEADD(MONTH, -1, GETDATE()) -- Filter transactions for the last one month
GROUP BY CONVERT(DATE, DOT)
ORDER BY TotalCashDeposits DESC;


--21.	List the clients who have not used their cheque books during the last 15 days
SELECT DISTINCT AM.NAME, AM.ACID
FROM AMASTER AM
WHERE AM.ACID NOT IN (
    SELECT DISTINCT T.ACID
    FROM TMASTER T
    WHERE T.TXNTYPE = 'CQD' -- Assuming 'CQD' represents cheque deposit transactions
    AND T.DOT >= DATEADD(DAY, -15, GETDATE()) -- Filter transactions for the last 15 days
);

--22.	List the transactions that have happened wherein the transacting branch is different from the branch 
--		in which the account is opened, but the Region is the same 
SELECT T.*, AM.BRID AS AccountBranchID, AM.NAME AS AccountHolderName
FROM TMASTER T
INNER JOIN AMASTER AM ON T.ACID = AM.ACID
INNER JOIN BRMASTER B1 ON T.BRID <> AM.BRID AND B1.BRID = T.BRID
INNER JOIN BRMASTER B2 ON B1.RID = B2.RID AND B2.BRID = AM.BRID;

--23.	List the transactions that have happened wherein the transacting branch is different from the branch in which 
--		the account is opened, and the two branches belong to different regions
SELECT T.*, AM.BRID AS AccountBranchID, AM.NAME AS AccountHolderName
FROM TMASTER T
INNER JOIN AMASTER AM ON T.ACID = AM.ACID
INNER JOIN BRMASTER TransactingBranch ON T.BRID <> AM.BRID AND TransactingBranch.BRID = T.BRID
INNER JOIN BRMASTER AccountBranch ON AccountBranch.BRID = AM.BRID
WHERE TransactingBranch.RID <> AccountBranch.RID;

--24.	List the average transaction amount, TransactionType-wise for a given branch and for a given date
DECLARE @BranchID VARCHAR(10) = 'BR1'; -- Replace 'YourBranchID' with the actual branch ID
DECLARE @Date DATE = '2024-04-04'; -- Replace '2024-04-01' with the actual date

SELECT TXNTYPE, AVG(TXNAMT) AS AverageTransactionAmount
FROM TMASTER
WHERE BRID = @BranchID
      AND CONVERT(DATE, DOT) = @Date
GROUP BY TXNTYPE;


--25.	Provide the following information from the ‘Account Master’ table:
--	Product-wise, month-wise, number of accounts
SELECT PID, 
       YEAR(DOO) AS Year, 
       MONTH(DOO) AS Month, 
       COUNT(ACID) AS NumAccounts
FROM AMASTER
GROUP BY PID, YEAR(DOO), MONTH(DOO)
ORDER BY PID, Year, Month;

--	Total number of accounts for each product
SELECT PID, 
       COUNT(ACID) AS TotalAccounts
FROM AMASTER
GROUP BY PID
ORDER BY PID;

--	Total number of accounts for each month
SELECT YEAR(DOO) AS Year, 
       MONTH(DOO) AS Month, 
       COUNT(ACID) AS TotalAccounts
FROM AMASTER
GROUP BY YEAR(DOO), MONTH(DOO)
ORDER BY Year, Month;

--	Total number of accounts in our bank
SELECT COUNT(ACID) AS TotalAccounts
FROM AMASTER;


/*
IV. STORED PROCEDURES 
-------------------------------------------------------------------------------------------				
						INDIAN BANK 
	List of Transactions from Feb 1st to 28, 2021 Report
-------------------------------------------------------------------------------------------
Product Name  : SB
Account No	:  	101							Branch: BR1
Customer Name:	Praveen S			     Cleared Balance	:1400
SL.NO	DATE	TXN TYPE	CHEQUE NO	AMOUNT	RUNNINGBALANCE
1      -              CD         -                 10000         10000
2    -                CD      -                   20000           30000
3                    CW                       5000               25000
-------------------------------------------------------------------------------------------
Total Number of Transactions :5
		Cash Deposits      :5
		Cash Withdrawals :0
		Cheque Deposits	:0
Dates when the Balance dropped below the Minimum Balance for the Product:
May 5, 2020
May 12, 2020
May 22, 2020
Closing Balance :1400
*/

 /*************************************************************************************
SP_Name	: previousmonthBankStatement  
Author	: Ankita Chougule
Date	: Mar 9th 2024
DB		: IBANK
Purpose : It will get previous month Trasnation done by given customers.

History:
---------------------------------------------------------------------------------------
SLNo	Done by				Date of change			Remarks
---------------------------------------------------------------------------------------
1		Ankita Chougule		Mar 9th 2024			New sp

****************************************************************************************/

create or alter proc usp_previousmonthBankStatement
(
	@acid int 
)
as
begin

	set nocount on; -- Add this line to suppress the row count message

	declare @CustName varchar(40)
	declare @pid char(2)
	declare @brid char(3)
	declare @balance money

	declare @rno int
	declare @DOT datetime
	declare @TXNTYPE char(3)
	declare @CHQNO int
	declare @TXNAMT money

	DECLARE @Lastmonth VARCHAR(40)
	DECLARE @TodaysDate DATETIME = GETDATE() -- '2021/6/10' 
	
	--get the last month name
	SET @Lastmonth = DATENAME(MM, DATEADD(MM, -1, @TodaysDate))

	--get 3 letters
	declare @@LastMont_Short varchar(3)
	select @@LastMont_Short = SUBSTRING(@Lastmonth, 1, 3)

	-- get LastMonthEndDate
	declare @LastMonthEndDate datetime
	select @LastMonthEndDate = EOMONTH(DATEADD(MM,-1,@TodaysDate))


	print'--------------------------------------------------------------------------------------------------'
	print '                       INDAIN BANK                              '
	print 'List of Transactions from ' + @@LastMont_Short +' 1st to '+ CONVERT(VARCHAR, @LastMonthEndDate,107) + ' Report'
	print'--------------------------------------------------------------------------------------------------'

	--1. Get customer info
	select @CustName = NAME,
		   @brid =  BRID,
		   @pid =  PID,
		@balance = CBAL 
		from AMASTER 
		where acid = @acid

		-- 2. Print the variables
		print 'Product Name : '  + @pid
		print 'Account No : ' + cast(@acid as varchar)  +space(30)+ 'Branch : ' + @brid
		print 'Customer Name : ' + @CustName	+space(26)+	 'Cleared balance : ' + cast(@balance as varchar) +' INR'
		print'--------------------------------------------------------------------------------------------------'
		print 'SL.NO	  DATE	   TXN TYPE	  CHEQUE NO	   AMOUNT	   RUNNINGBALANCE'
		print'--------------------------------------------------------------------------------------------------'

		-- 3. Get previous month transactions done by the given cutomer. Store the data in temp table
		select ROW_NUMBER() over(order by DOT asc) as rno, DOT, TXNTYPE, CHQNO, TXNAMT
		into #txndata
		from TMASTER where DATEDIFF(MM, DOT, GETDATE()) = 1 AND ACID = @acid

		--4. print the data from temp table
		--select * from #txndata

		-- 5. Loop
		declare @x int
		set @x = 1

		declare @cnt int

		--condition
		select @cnt = count(*) from #txndata

		-- loop syntax
		while (@x <= @cnt)
		begin
			-- get a single row
			select @rno = rno, 
				   @DOT = DOT, 
				   @TXNTYPE = TXNTYPE, 
				   @CHQNO = CHQNO, 
				   @TXNAMT = TXNAMT 
			from #txndata where rno = @x

			--print the data
			print cast(@rno as varchar) + space(5) +  convert(varchar, @dot, 107) +
			space(5)+ @TXNTYPE +space(7)+ cast(isnull(@CHQNO,0) as varchar) +
			space(8)+cast(@TXNAMT as varchar)

			--incr
			set @x = @x + 1

		end -- loop end

		print'--------------------------------------------------------------------------------------------------'

		print 'Total Number of Transactions :     ' + cast(@cnt as varchar)
		
		declare @CDs int
		select @CDs = count(*) from #txndata where TXNTYPE = 'CD'
		print 'Total Number of Cash Deposits :    ' + cast(@CDs as varchar)

		declare @CWs int
		select @CWs = count(*) from #txndata where TXNTYPE = 'CD'
		print 'Total Number of Cash Withdrawals : ' + cast(@CWs as varchar)

		declare @CQDs int
		select @CQDs = count(*) from #txndata where TXNTYPE = 'CD'
		print 'Total Number of Cheque Deposits	:  ' + cast(@CQDs as varchar)
		
		print'--------------------------------------------------------------------------------------------------'
		PRINT 'Thanks for Banking with us. For more help call our customer care : 1000 123 3334'
		print'--------------------------------------------------------------------------------------------------'
end
go

exec usp_previousmonthBankStatement 101


/*
Requirement:-
Input Parameter
@LoanAmt = 200000
@ROI = 12
@TenureInYrs = 3

1) Get the ineterest
PNR/100
declare @IntAmt money
set @IntAmt = (@LoanAmt * @ROI * @TenureInYrs)/100

2) Totak Amt
declare @TotalAmt money
set @TotalAmt = @LoanAmt + @IntAmt

3) Get EMIAmt
declare @EMIAmt money
set @EMIAmt = @TotalAmt /(@TenureInYrs * 12)

--------------------------------------------------------------------------------
MonthNo				DateOfEMI			EMI_Amt
--------------------------------------------------------------------------------
1					Apr 3 2024			 @EMIAmt
2					May 3 2024			 @EMIAmt
--------------------------------------------------------------------------------

*/

/*************************************************************************************
SP_Name	: LoanStatement 
Author	: Ankita Chougule
Date	: Mar 10th 2024
DB		: IBANK

Purpose : It will prepare the loan statement

History:
---------------------------------------------------------------------------------------
SLNo	Done by				Date of change			Remarks
---------------------------------------------------------------------------------------
1		Ankita Chougule		Mar 10th 2024			New sp

****************************************************************************************/

use IBank

create or alter proc usp_LoanStatement
(
	@LoanAmt money,
	@ROI tinyint,
	@TenureInYrs tinyint
)
as
begin
	
	--1) Get the ineterest : PNR/100
	declare @IntAmt money
	set @IntAmt = (@LoanAmt * @ROI * @TenureInYrs)/100

	-- print @IntAmt

	--2) Totak Amt
	declare @TotalAmt money
	set @TotalAmt = @LoanAmt + @IntAmt
	-- print @TotalAmt

	--3) Get EMIAmt
	declare @EMIAmt money
	set @EMIAmt = @TotalAmt /(@TenureInYrs * 12)
	-- print @EMIAmt

	--Loan date
	declare @LoanDate datetime
	set @LoanDate = GETDATE()

	print'				Loan Statement Table							'

	print'--------------------------------------------------------------------------------'
	print'MonthNo		DateOfEMI			EMI_Amt'
	print'--------------------------------------------------------------------------------'

	--loop
	declare @x int
	set @x = 1

	while (@x <= (@TenureInYrs * 12))
	begin
		--action
		print cast(@x as varchar) + space(10) + 
		convert(varchar,dateadd(mm,@x,@LoanDate),107) + space(10) + 
		cast(@EMIAmt as varchar)

		--incr
		set @x = @x + 1
	end --end of loop

	print'--------------------------------------------------------------------------------'
	print 'Total Amount : INR ' + cast (@TotalAmt as varchar)
	print'--------------------------------------------------------------------------------'

end

exec usp_LoanStatement 200000,12,3

select * from sys.tables where name = 'amaster'
select * from RMASTER
select * from BRMASTER
select * from AMASTER 
select * from UMASTER
select * from TMASTER 
select * from HIGH_TMASTER
select * from PMASTER

select 'select * from ' + name from sys.views

select * from vw_BranchWiseCustomers
select * from Vw_GetCustInfo
select * from Vw_GetCustTxnsInfo
select * from vw_getBR5custInfo
select * from vw_customers_BR1
select * from vw_CurrentYearTxns
select * from vw_GetCustNameAndTxns
select * from vw_GetBR1FDCustomers
select * from vw_GetProductWiseTotalAmt
select * from vw_GetMyBalance
select * from vw_NextMonth_InOperativeAccts
select * from SBCustomers

select * from sys.procedures 

sp_helptext 'vw_getBR5custInfo'

select * from sys.triggers where parent_id = 165575628



