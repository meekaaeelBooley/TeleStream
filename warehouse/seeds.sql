-- GENERATED FILE — do not edit by hand.
-- Regenerate with: python warehouse/generate_seeds.py

INSERT INTO dim_tower (tower_id, tower_name, province, technologies) VALUES
    ('CPT-CBD-001', 'Cape Town CBD', 'Western Cape', ARRAY['3G', 'LTE', '5G']),
    ('CPT-BLV-003', 'Bellville', 'Western Cape', ARRAY['3G', 'LTE']),
    ('CPT-CC-002', 'Century City', 'Western Cape', ARRAY['LTE', '5G']),
    ('CPT-STB-004', 'Stellenbosch', 'Western Cape', ARRAY['3G', 'LTE']),
    ('JHB-SDT-001', 'Sandton', 'Gauteng', ARRAY['LTE', '5G']),
    ('JHB-CBD-002', 'Johannesburg CBD', 'Gauteng', ARRAY['3G', 'LTE', '5G']),
    ('JHB-SWT-003', 'Soweto', 'Gauteng', ARRAY['3G', 'LTE']),
    ('PTA-CBD-001', 'Pretoria CBD', 'Gauteng', ARRAY['3G', 'LTE', '5G']),
    ('DBN-CBD-001', 'Durban CBD', 'KwaZulu-Natal', ARRAY['3G', 'LTE', '5G']),
    ('DBN-UMH-002', 'Umhlanga', 'KwaZulu-Natal', ARRAY['LTE', '5G']),
    ('PMB-CBD-001', 'Pietermaritzburg', 'KwaZulu-Natal', ARRAY['3G', 'LTE']),
    ('GQB-CBD-001', 'Gqeberha Central', 'Eastern Cape', ARRAY['3G', 'LTE']),
    ('EL-CBD-001', 'East London', 'Eastern Cape', ARRAY['3G', 'LTE']),
    ('BFN-CBD-001', 'Bloemfontein', 'Free State', ARRAY['3G', 'LTE']),
    ('PLK-CBD-001', 'Polokwane', 'Limpopo', ARRAY['3G', 'LTE']),
    ('NLP-CBD-001', 'Mbombela', 'Mpumalanga', ARRAY['3G', 'LTE']),
    ('KIM-CBD-001', 'Kimberley', 'Northern Cape', ARRAY['3G']),
    ('RTB-CBD-001', 'Rustenburg', 'North West', ARRAY['3G', 'LTE']);

INSERT INTO dim_bundle (bundle_code, bundle_name, bundle_type, price) VALUES
    ('DATA_1GB', '1GB Data', 'DATA', 99.00),
    ('DATA_5GB', '5GB Data', 'DATA', 199.00),
    ('DATA_20GB', '20GB Data', 'DATA', 399.00),
    ('VOICE_100MIN', '100 Minutes', 'VOICE', 79.00),
    ('VOICE_300MIN', '300 Minutes', 'VOICE', 169.00),
    ('SMS_500', '500 SMS', 'SMS', 49.00),
    ('COMBO_STARTER', 'Starter Combo', 'COMBO', 149.00),
    ('COMBO_POWER', 'Power Combo', 'COMBO', 299.00);
