USE [master];
GO

-- Create the database if it does not exist
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'ActiveMQArtemisDb')
BEGIN
    CREATE DATABASE [ActiveMQArtemisDb];
END
GO

USE [ActiveMQArtemisDb];
GO

-- Create the ACKS_JOURNAL table if it does not exist
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='ACKS_JOURNAL' AND xtype='U')
BEGIN
    CREATE TABLE [dbo].[ACKS_JOURNAL](
        [ID] [bigint] NOT NULL,
        [RECORD_TYPE] [tinyint] NOT NULL,
        [PAYLOAD] [varbinary](max) NOT NULL,
        [TXID] [bigint] NULL,
        PRIMARY KEY CLUSTERED 
        (
            [ID] ASC
        ) WITH (
            PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, 
            IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, 
            ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF
        ) ON [PRIMARY]
    ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY];
END
GO

-- Create the BINDINGS_JOURNAL table if it does not exist
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='BINDINGS_JOURNAL' AND xtype='U')
BEGIN
    CREATE TABLE [dbo].BINDINGS_JOURNAL(
        [ID] [bigint] NOT NULL,
        [RECORD_TYPE] [tinyint] NOT NULL,
        [PAYLOAD] [varbinary](max) NOT NULL,
        [TXID] [bigint] NULL,
        PRIMARY KEY CLUSTERED 
        (
            [ID] ASC
        ) WITH (
            PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, 
            IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, 
            ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF
        ) ON [PRIMARY]
    ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY];
END
GO

-- Create the LARGE_MESSAGES table if it does not exist
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='LARGE_MESSAGES' AND xtype='U')
BEGIN
    CREATE TABLE [dbo].LARGE_MESSAGES(
        [ID] [bigint] NOT NULL,
        [RECORD_TYPE] [tinyint] NOT NULL,
        [PAYLOAD] [varbinary](max) NOT NULL,
        [TXID] [bigint] NULL,
        PRIMARY KEY CLUSTERED 
        (
            [ID] ASC
        ) WITH (
            PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, 
            IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, 
            ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF
        ) ON [PRIMARY]
    ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY];
END
GO


-- Create the MESSAGES_JOURNAL table if it does not exist
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='MESSAGES_JOURNAL' AND xtype='U')
BEGIN
    CREATE TABLE [dbo].MESSAGES_JOURNAL(
        [ID] [bigint] NOT NULL,
        [RECORD_TYPE] [tinyint] NOT NULL,
        [PAYLOAD] [varbinary](max) NOT NULL,
        [TXID] [bigint] NULL,
        PRIMARY KEY CLUSTERED 
        (
            [ID] ASC
        ) WITH (
            PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, 
            IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, 
            ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF
        ) ON [PRIMARY]
    ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY];
END
GO

-- Create the NODE_MANAGER_STORE table if it does not exist
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='NODE_MANAGER_STORE' AND xtype='U')
BEGIN
    CREATE TABLE [dbo].NODE_MANAGER_STORE(
        [ID] [bigint] NOT NULL,
        [RECORD_TYPE] [tinyint] NOT NULL,
        [PAYLOAD] [varbinary](max) NOT NULL,
        [TXID] [bigint] NULL,
        PRIMARY KEY CLUSTERED 
        (
            [ID] ASC
        ) WITH (
            PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, 
            IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, 
            ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF
        ) ON [PRIMARY]
    ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY];
END
GO

