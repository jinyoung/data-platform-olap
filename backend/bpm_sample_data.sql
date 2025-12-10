-- BPM Process Performance Sample Data
-- Drop existing tables if they exist
DROP TABLE IF EXISTS fact_process_instance CASCADE;
DROP TABLE IF EXISTS dim_activity CASCADE;
DROP TABLE IF EXISTS dim_user CASCADE;
DROP TABLE IF EXISTS dim_time CASCADE;

-- Create Time Dimension
CREATE TABLE dim_time (
    id SERIAL PRIMARY KEY,
    year INTEGER NOT NULL,
    quarter VARCHAR(2) NOT NULL,
    month VARCHAR(20) NOT NULL,
    month_num INTEGER NOT NULL
);

-- Create Activity Dimension (Process > Activity hierarchy)
CREATE TABLE dim_activity (
    id SERIAL PRIMARY KEY,
    process_name VARCHAR(100) NOT NULL,
    activity_name VARCHAR(100) NOT NULL,
    activity_type VARCHAR(50)
);

-- Create User Dimension (Department > User hierarchy)
CREATE TABLE dim_user (
    id SERIAL PRIMARY KEY,
    department VARCHAR(100) NOT NULL,
    user_name VARCHAR(100) NOT NULL,
    role VARCHAR(50)
);

-- Create Fact Table
CREATE TABLE fact_process_instance (
    id SERIAL PRIMARY KEY,
    instance_id VARCHAR(50) NOT NULL,
    activity_id INTEGER REFERENCES dim_activity(id),
    user_id INTEGER REFERENCES dim_user(id),
    time_id INTEGER REFERENCES dim_time(id),
    work_time_minutes INTEGER NOT NULL,
    lead_time_minutes INTEGER NOT NULL,
    rework_count INTEGER DEFAULT 0,
    satisfaction_score DECIMAL(3,2)
);

-- Insert Time Dimension Data (2023-2024)
INSERT INTO dim_time (year, quarter, month, month_num) VALUES
-- 2023
(2023, 'Q1', 'January', 1),
(2023, 'Q1', 'February', 2),
(2023, 'Q1', 'March', 3),
(2023, 'Q2', 'April', 4),
(2023, 'Q2', 'May', 5),
(2023, 'Q2', 'June', 6),
(2023, 'Q3', 'July', 7),
(2023, 'Q3', 'August', 8),
(2023, 'Q3', 'September', 9),
(2023, 'Q4', 'October', 10),
(2023, 'Q4', 'November', 11),
(2023, 'Q4', 'December', 12),
-- 2024
(2024, 'Q1', 'January', 1),
(2024, 'Q1', 'February', 2),
(2024, 'Q1', 'March', 3),
(2024, 'Q2', 'April', 4),
(2024, 'Q2', 'May', 5),
(2024, 'Q2', 'June', 6),
(2024, 'Q3', 'July', 7),
(2024, 'Q3', 'August', 8),
(2024, 'Q3', 'September', 9),
(2024, 'Q4', 'October', 10),
(2024, 'Q4', 'November', 11),
(2024, 'Q4', 'December', 12);

-- Insert Activity Dimension Data (Processes and Activities)
INSERT INTO dim_activity (process_name, activity_name, activity_type) VALUES
-- 주문처리 프로세스
('주문처리', '주문접수', 'UserTask'),
('주문처리', '재고확인', 'ServiceTask'),
('주문처리', '결제처리', 'UserTask'),
('주문처리', '배송준비', 'UserTask'),
('주문처리', '배송완료', 'EndEvent'),
-- 고객지원 프로세스
('고객지원', '문의접수', 'UserTask'),
('고객지원', '문의분류', 'ServiceTask'),
('고객지원', '담당자배정', 'UserTask'),
('고객지원', '문제해결', 'UserTask'),
('고객지원', '만족도조사', 'UserTask'),
-- 구매요청 프로세스
('구매요청', '요청서작성', 'UserTask'),
('구매요청', '부서장승인', 'UserTask'),
('구매요청', '재무검토', 'UserTask'),
('구매요청', '최종승인', 'UserTask'),
('구매요청', '발주처리', 'ServiceTask'),
-- 휴가신청 프로세스
('휴가신청', '신청서작성', 'UserTask'),
('휴가신청', '팀장승인', 'UserTask'),
('휴가신청', '인사팀검토', 'UserTask'),
('휴가신청', '최종확정', 'EndEvent');

-- Insert User Dimension Data (Departments and Users)
INSERT INTO dim_user (department, user_name, role) VALUES
-- 영업부
('영업부', '김영희', 'Manager'),
('영업부', '이철수', 'Staff'),
('영업부', '박지민', 'Staff'),
('영업부', '최수진', 'Staff'),
-- 고객서비스부
('고객서비스부', '정미영', 'Manager'),
('고객서비스부', '한동욱', 'Staff'),
('고객서비스부', '강서연', 'Staff'),
('고객서비스부', '윤재호', 'Staff'),
-- 구매부
('구매부', '송민준', 'Manager'),
('구매부', '임소연', 'Staff'),
('구매부', '조현우', 'Staff'),
-- 인사부
('인사부', '백지훈', 'Manager'),
('인사부', '신예진', 'Staff'),
-- 재무부
('재무부', '황승현', 'Manager'),
('재무부', '오다영', 'Staff');

-- Generate Process Instance Data
-- We'll create realistic BPM data with varying performance

-- Function to generate random data
DO $$
DECLARE
    v_instance_id INTEGER := 1;
    v_time_id INTEGER;
    v_activity_id INTEGER;
    v_user_id INTEGER;
    v_work_time INTEGER;
    v_lead_time INTEGER;
    v_rework INTEGER;
    v_satisfaction DECIMAL;
    v_process_type TEXT;
BEGIN
    -- Loop through each time period
    FOR v_time_id IN 1..24 LOOP
        -- Generate instances for each process type
        
        -- 주문처리 프로세스 (높은 볼륨)
        FOR i IN 1..FLOOR(RANDOM() * 30 + 40)::INTEGER LOOP
            FOR v_activity_id IN 1..5 LOOP
                -- Random user from 영업부 (1-4)
                v_user_id := FLOOR(RANDOM() * 4 + 1)::INTEGER;
                v_work_time := FLOOR(RANDOM() * 30 + 10)::INTEGER;
                v_lead_time := FLOOR(RANDOM() * 120 + 30)::INTEGER;
                v_rework := CASE WHEN RANDOM() < 0.15 THEN FLOOR(RANDOM() * 3 + 1)::INTEGER ELSE 0 END;
                v_satisfaction := ROUND((RANDOM() * 2 + 3)::NUMERIC, 2);
                
                INSERT INTO fact_process_instance 
                    (instance_id, activity_id, user_id, time_id, work_time_minutes, lead_time_minutes, rework_count, satisfaction_score)
                VALUES 
                    ('ORD-' || LPAD(v_instance_id::TEXT, 6, '0'), v_activity_id, v_user_id, v_time_id, v_work_time, v_lead_time, v_rework, v_satisfaction);
            END LOOP;
            v_instance_id := v_instance_id + 1;
        END LOOP;
        
        -- 고객지원 프로세스 (중간 볼륨)
        FOR i IN 1..FLOOR(RANDOM() * 20 + 25)::INTEGER LOOP
            FOR v_activity_id IN 6..10 LOOP
                -- Random user from 고객서비스부 (5-8)
                v_user_id := FLOOR(RANDOM() * 4 + 5)::INTEGER;
                v_work_time := FLOOR(RANDOM() * 45 + 15)::INTEGER;
                v_lead_time := FLOOR(RANDOM() * 240 + 60)::INTEGER;
                v_rework := CASE WHEN RANDOM() < 0.2 THEN FLOOR(RANDOM() * 2 + 1)::INTEGER ELSE 0 END;
                v_satisfaction := ROUND((RANDOM() * 2.5 + 2.5)::NUMERIC, 2);
                
                INSERT INTO fact_process_instance 
                    (instance_id, activity_id, user_id, time_id, work_time_minutes, lead_time_minutes, rework_count, satisfaction_score)
                VALUES 
                    ('SUP-' || LPAD(v_instance_id::TEXT, 6, '0'), v_activity_id, v_user_id, v_time_id, v_work_time, v_lead_time, v_rework, v_satisfaction);
            END LOOP;
            v_instance_id := v_instance_id + 1;
        END LOOP;
        
        -- 구매요청 프로세스 (낮은 볼륨, 긴 리드타임)
        FOR i IN 1..FLOOR(RANDOM() * 10 + 8)::INTEGER LOOP
            FOR v_activity_id IN 11..15 LOOP
                -- Random user from 구매부/재무부 (9-15)
                v_user_id := FLOOR(RANDOM() * 6 + 9)::INTEGER;
                v_work_time := FLOOR(RANDOM() * 60 + 20)::INTEGER;
                v_lead_time := FLOOR(RANDOM() * 480 + 120)::INTEGER;
                v_rework := CASE WHEN RANDOM() < 0.25 THEN FLOOR(RANDOM() * 2 + 1)::INTEGER ELSE 0 END;
                v_satisfaction := ROUND((RANDOM() * 1.5 + 3)::NUMERIC, 2);
                
                INSERT INTO fact_process_instance 
                    (instance_id, activity_id, user_id, time_id, work_time_minutes, lead_time_minutes, rework_count, satisfaction_score)
                VALUES 
                    ('PUR-' || LPAD(v_instance_id::TEXT, 6, '0'), v_activity_id, v_user_id, v_time_id, v_work_time, v_lead_time, v_rework, v_satisfaction);
            END LOOP;
            v_instance_id := v_instance_id + 1;
        END LOOP;
        
        -- 휴가신청 프로세스 (낮은 볼륨)
        FOR i IN 1..FLOOR(RANDOM() * 8 + 5)::INTEGER LOOP
            FOR v_activity_id IN 16..19 LOOP
                -- Random user from 인사부 (12-13)
                v_user_id := FLOOR(RANDOM() * 2 + 12)::INTEGER;
                v_work_time := FLOOR(RANDOM() * 15 + 5)::INTEGER;
                v_lead_time := FLOOR(RANDOM() * 60 + 15)::INTEGER;
                v_rework := CASE WHEN RANDOM() < 0.1 THEN 1 ELSE 0 END;
                v_satisfaction := ROUND((RANDOM() * 1 + 4)::NUMERIC, 2);
                
                INSERT INTO fact_process_instance 
                    (instance_id, activity_id, user_id, time_id, work_time_minutes, lead_time_minutes, rework_count, satisfaction_score)
                VALUES 
                    ('LEV-' || LPAD(v_instance_id::TEXT, 6, '0'), v_activity_id, v_user_id, v_time_id, v_work_time, v_lead_time, v_rework, v_satisfaction);
            END LOOP;
            v_instance_id := v_instance_id + 1;
        END LOOP;
        
    END LOOP;
END $$;

-- Create indexes for better query performance
CREATE INDEX idx_fact_activity ON fact_process_instance(activity_id);
CREATE INDEX idx_fact_user ON fact_process_instance(user_id);
CREATE INDEX idx_fact_time ON fact_process_instance(time_id);
CREATE INDEX idx_fact_instance ON fact_process_instance(instance_id);

-- Verify data counts
SELECT 'dim_time' as table_name, COUNT(*) as row_count FROM dim_time
UNION ALL
SELECT 'dim_activity', COUNT(*) FROM dim_activity
UNION ALL
SELECT 'dim_user', COUNT(*) FROM dim_user
UNION ALL
SELECT 'fact_process_instance', COUNT(*) FROM fact_process_instance;

