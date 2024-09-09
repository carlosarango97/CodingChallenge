CREATE TABLE [dim].[hired_employees](
	[id] [int] NOT NULL,
	[name] [varchar](255) NULL,
    [datetime] [datetime] NOT NULL,
    [department_id] [int] NULL,
    [job_id] [int] NULL,
    CONSTRAINT PK_hired_employees_id PRIMARY KEY CLUSTERED (id),
    CONSTRAINT FK_hired_employees__job FOREIGN KEY (job_id) REFERENCES dim.job(id) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT FK_hired_employees__department FOREIGN KEY (department_id) REFERENCES dim.department(id) ON DELETE CASCADE ON UPDATE CASCADE
)