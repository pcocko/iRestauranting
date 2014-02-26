-- --------------------------------------------------------------------------------
-- Routine DDL
-- Note: comments before and after the routine body will not be stored by the server
-- --------------------------------------------------------------------------------
DELIMITER $$

CREATE DEFINER=`62465_evila`@`%` PROCEDURE `calc_score_global_all`(in_id_restaurant INT(11), in_dt_calc DATE, in_dt_calc_start DATE, in_dt_calc_end DATE)
BEGIN

	DECLARE v_num_process_in INT(11);
	DECLARE v_num_process_out INT(11);

	DECLARE v_id_restaurant INT(11);
	DECLARE v_id_country_3 VARCHAR(3) DEFAULT NULL;
	DECLARE v_id_admin_area_l1 VARCHAR(200) DEFAULT NULL;
	DECLARE v_id_admin_area_l2 VARCHAR(200) DEFAULT NULL;
	DECLARE v_id_locality VARCHAR(200) DEFAULT NULL;
	DECLARE v_id_zip_code VARCHAR(5) DEFAULT NULL;

	DECLARE v_ds_rest_url VARCHAR(200);
	DECLARE v_ds_rest_facebook VARCHAR(200);
	DECLARE v_ds_rest_twitter VARCHAR(200);
	DECLARE v_restaurants_done INT DEFAULT 0;
	DECLARE v_date_done INT DEFAULT 0;
	DECLARE v_dt_calc DATE;
	DECLARE v_min_dt_score DATE;
	DECLARE v_dt_calc_end DATE;
	DECLARE v_dt_last_calc DATE;
	DECLARE c_initial_calc DATE DEFAULT DATE_ADD(DATE(now()), INTERVAL -2 DAY);
	DECLARE v_count_1 INT(10) DEFAULT 0;
	DECLARE v_count_2 INT(10) DEFAULT 0;
	DECLARE cur_mast_rest CURSOR FOR SELECT ID_RESTAURANT, DS_REST_URL, DS_REST_FACEBOOK, DS_REST_TWITTER, ID_COUNTRY_3, ID_ADMIN_AREA_L1, ID_ADMIN_AREA_L2, ID_LOCALITY, ID_ZIP_CODE FROM MAST_RESTAURANTS WHERE BL_ACTIVE = 'Y';
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_restaurants_done = 1;
		
 	Call create_log_process ('START', NULL, 'DB', 'CALC_SCORE_GLOBAL_ALL', NULL,NULL, v_num_process_out);

	Call create_log_detail(v_num_process_out, 'DB', 'CALC_SCORE_GLOBAL_ALL', 'INFO', CONCAT('RESTAURANTS: ', in_id_restaurant,' | ONLY: ', in_dt_calc,' | START: ', in_dt_calc_start, ' | END: ', in_dt_calc_end));


	IF in_id_restaurant = 0 THEN

		SET v_restaurants_done = 0;

		OPEN cur_mast_rest;

		rest_loop: LOOP
			FETCH cur_mast_rest INTO v_id_restaurant, v_ds_rest_url, v_ds_rest_facebook, v_ds_rest_twitter, v_id_country_3, v_id_admin_area_l1, v_id_admin_area_l2, v_id_locality, v_id_zip_code;

			IF v_restaurants_done THEN
				LEAVE rest_loop;
			END IF;

			
			IF (in_dt_calc <> '0000-00-00' AND in_dt_calc_start = '0000-00-00' AND in_dt_calc_end = '0000-00-00') THEN

				SET v_dt_calc = in_dt_calc;

				CALL calc_score_global_one (v_id_restaurant, v_ds_rest_url, v_ds_rest_facebook, v_ds_rest_twitter, v_id_country_3, v_id_admin_area_l1, v_id_admin_area_l2, v_id_locality, v_id_zip_code, v_dt_calc, v_num_process_out);

			END IF;

			
			IF (in_dt_calc = '0000-00-00' AND in_dt_calc_start <> '0000-00-00' AND in_dt_calc_end <> '0000-00-00') THEN

				SET v_date_done = 0 ;
				SET v_dt_calc = in_dt_calc_start;

				loop_time: LOOP

					IF v_dt_calc > in_dt_calc_end THEN 
						SET v_date_done = 1 ;
					END IF;

					IF v_date_done THEN
						LEAVE loop_time;
					END IF;	

					CALL calc_score_global_one (v_id_restaurant, v_ds_rest_url, v_ds_rest_facebook, v_ds_rest_twitter, v_id_country_3, v_id_admin_area_l1, v_id_admin_area_l2, v_id_locality, v_id_zip_code, v_dt_calc, v_num_process_out);

					SET v_dt_calc = DATE_ADD(v_dt_calc, INTERVAL 1 DAY);

				END LOOP;
			END IF;

			
			IF (in_dt_calc = '0000-00-00' AND in_dt_calc_start = '0000-00-00' AND in_dt_calc_end = '0000-00-00') THEN

				SELECT max(DT_SCORE) INTO v_dt_calc_end FROM HIST_SCORE_REVIEWS WHERE ID_RESTAURANT = v_id_restaurant;
				-- PABLO / No hay fecha de fin porque no hay opiniones 
				IF v_dt_calc_end IS NULL THEN
					SET v_dt_calc_end = c_initial_calc;
				END IF;
				-- END PABLO

				SELECT count(*) INTO v_count_1 FROM AUX_CTRL_SCORE_GLOBAL WHERE ID_RESTAURANT = v_id_restaurant;

				IF v_count_1 = 0 THEN
					SELECT MIN(DT_SCORE) INTO v_min_dt_score FROM HIST_SCORE_REVIEWS WHERE ID_RESTAURANT = v_id_restaurant;
					-- PABLO / No hay fecha de fin porque no hay opiniones 
					IF v_min_dt_score IS NULL THEN
						SET v_min_dt_score = c_initial_calc;
					END IF;
					-- END PABLO

					SET v_dt_last_calc = DATE_ADD(v_min_dt_score, INTERVAL -1 DAY);

					INSERT INTO AUX_CTRL_SCORE_GLOBAL (ID_RESTAURANT, DT_MAX_SCORE) VALUES (v_id_restaurant, v_dt_last_calc);
				ELSE
					SELECT DT_MAX_SCORE INTO v_dt_last_calc FROM AUX_CTRL_SCORE_GLOBAL WHERE ID_RESTAURANT = v_id_restaurant;
					IF v_dt_last_calc IS NULL THEN
						SELECT min(DT_SCORE) INTO v_dt_last_calc FROM HIST_SCORE_REVIEWS WHERE ID_RESTAURANT = v_id_restaurant;
						IF v_dt_last_calc IS NOT NULL THEN
							UPDATE AUX_CTRL_SCORE_GLOBAL SET DT_MAX_SCORE = DATE_ADD(v_dt_last_calc, INTERVAL -1 DAY) WHERE ID_RESTAURANT = v_id_restaurant;
						ELSE
							SET v_dt_last_calc = DATE_ADD(c_initial_calc, INTERVAL -1 DAY);
						END IF;
					END IF;
				END IF;

				IF v_dt_last_calc IS NULL THEN
					SET v_date_done = 1;
				ELSE
					SET v_date_done = 0;
				END IF;

				SET v_dt_calc = v_dt_last_calc;

				loop_time: LOOP

					IF v_dt_calc > v_dt_calc_end THEN 
						SET v_date_done = 1 ;
					END IF;

					IF v_date_done THEN
						LEAVE loop_time;
					END IF;	

					CALL calc_score_global_one (v_id_restaurant, v_ds_rest_url, v_ds_rest_facebook, v_ds_rest_twitter, v_id_country_3, v_id_admin_area_l1, v_id_admin_area_l2, v_id_locality, v_id_zip_code, v_dt_calc, v_num_process_out);

					SET v_dt_calc = DATE_ADD(v_dt_calc, INTERVAL 1 DAY);

				END LOOP;





			END IF;

			UPDATE AUX_CTRL_SCORE_GLOBAL A 
				SET A.DT_MIN_SCORE = IFNULL((SELECT min(B.DT_SCORE) FROM HIST_SCORE_GLOBAL B WHERE A.ID_RESTAURANT = B.ID_RESTAURANT),c_initial_calc),
					A.DT_MAX_SCORE = IFNULL((SELECT max(B.DT_SCORE) FROM HIST_SCORE_GLOBAL B WHERE A.ID_RESTAURANT = B.ID_RESTAURANT),DATE_ADD(v_min_dt_score, INTERVAL 1 DAY))
				WHERE A.ID_RESTAURANT =v_id_restaurant;

		END LOOP;

		CLOSE cur_mast_rest;

	END IF;


	IF in_id_restaurant <> 0 THEN


		SELECT count(*) INTO v_count_2 FROM MAST_RESTAURANTS WHERE ID_RESTAURANT = in_id_restaurant;
		IF v_count_2 = 0 THEN
			Call create_log_detail(v_num_process_out, 'DB', 'CALC_SCORE_GLOBAL_ALL', 'INFO', CONCAT('WARNING: RESTAURANT ', in_id_restaurant,' DOES NOT EXISTS'));
		ELSE
			SET v_id_restaurant = in_id_restaurant;
			SELECT DS_REST_URL, DS_REST_FACEBOOK, DS_REST_TWITTER, ID_COUNTRY_3, ID_ADMIN_AREA_L1, ID_ADMIN_AREA_L2, ID_LOCALITY, ID_ZIP_CODE INTO v_ds_rest_url, v_ds_rest_facebook, v_ds_rest_twitter, v_id_country_3, v_id_admin_area_l1, v_id_admin_area_l2, v_id_locality, v_id_zip_code FROM MAST_RESTAURANTS WHERE ID_RESTAURANT = in_id_restaurant;

			
			IF (in_dt_calc <> '0000-00-00' AND in_dt_calc_start = '0000-00-00' AND in_dt_calc_end = '0000-00-00') THEN


				SET v_dt_calc = in_dt_calc;

				CALL calc_score_global_one (v_id_restaurant, v_ds_rest_url, v_ds_rest_facebook, v_ds_rest_twitter, v_id_country_3, v_id_admin_area_l1, v_id_admin_area_l2, v_id_locality, v_id_zip_code, v_dt_calc, v_num_process_out);

			END IF;

			
			IF (in_dt_calc = '0000-00-00' AND in_dt_calc_start <> '0000-00-00' AND in_dt_calc_end <> '0000-00-00') THEN


				SET v_date_done = 0 ;
				SET v_dt_calc = in_dt_calc_start;

				loop_time: LOOP

					IF v_dt_calc > in_dt_calc_end THEN 
						SET v_date_done = 1 ;
					END IF;

					IF v_date_done THEN
						LEAVE loop_time;
					END IF;	

					CALL calc_score_global_one (v_id_restaurant, v_ds_rest_url, v_ds_rest_facebook, v_ds_rest_twitter, v_id_country_3, v_id_admin_area_l1, v_id_admin_area_l2, v_id_locality, v_id_zip_code, v_dt_calc, v_num_process_out);

					SET v_dt_calc = DATE_ADD(v_dt_calc, INTERVAL 1 DAY);

				END LOOP;
			END IF;

			
			IF (in_dt_calc = '0000-00-00' AND in_dt_calc_start = '0000-00-00' AND in_dt_calc_end = '0000-00-00') THEN

				SELECT max(DT_SCORE) INTO v_dt_calc_end FROM HIST_SCORE_REVIEWS WHERE ID_RESTAURANT = v_id_restaurant;
				
				

				SELECT count(*) INTO v_count_1 FROM AUX_CTRL_SCORE_GLOBAL WHERE ID_RESTAURANT = v_id_restaurant;

				IF v_count_1 = 0 THEN
					SELECT MIN(DT_SCORE) INTO v_min_dt_score FROM HIST_SCORE_REVIEWS WHERE ID_RESTAURANT = v_id_restaurant;
					SET v_dt_last_calc = DATE_ADD(v_min_dt_score, INTERVAL -1 DAY);
					INSERT INTO AUX_CTRL_SCORE_GLOBAL (ID_RESTAURANT, DT_MAX_SCORE) VALUES (v_id_restaurant, v_dt_last_calc);
				ELSE
					SELECT DT_MAX_SCORE INTO v_dt_last_calc FROM AUX_CTRL_SCORE_GLOBAL WHERE ID_RESTAURANT = v_id_restaurant;
					IF v_dt_last_calc IS NULL THEN 
						SELECT min(DT_SCORE) INTO v_dt_last_calc FROM HIST_SCORE_REVIEWS WHERE ID_RESTAURANT = v_id_restaurant;
						IF v_dt_last_calc IS NOT NULL THEN
							UPDATE AUX_CTRL_SCORE_GLOBAL SET DT_MAX_SCORE = DATE_ADD(v_dt_last_calc, INTERVAL -1 DAY) WHERE ID_RESTAURANT = v_id_restaurant;
						END IF;
					END IF;
				END IF;
				
				IF v_dt_last_calc IS NULL THEN
					SET v_date_done = 1;
				ELSE
					SET v_date_done = 0;
				END IF;

				SET v_dt_calc = v_dt_last_calc;

				loop_time: LOOP

					IF v_dt_calc > v_dt_calc_end THEN 
						SET v_date_done = 1 ;
					END IF;

					IF v_date_done THEN
						LEAVE loop_time;
					END IF;	
	
					CALL calc_score_global_one (v_id_restaurant, v_ds_rest_url, v_ds_rest_facebook, v_ds_rest_twitter, v_id_country_3, v_id_admin_area_l1, v_id_admin_area_l2, v_id_locality, v_id_zip_code, v_dt_calc, v_num_process_out);

					SET v_dt_calc = DATE_ADD(v_dt_calc, INTERVAL 1 DAY);

				END LOOP;



			END IF;

			UPDATE AUX_CTRL_SCORE_GLOBAL A 
				SET A.DT_MIN_SCORE = (SELECT min(B.DT_SCORE) FROM HIST_SCORE_GLOBAL B WHERE A.ID_RESTAURANT = B.ID_RESTAURANT),
					A.DT_MAX_SCORE = (SELECT max(B.DT_SCORE) FROM HIST_SCORE_GLOBAL B WHERE A.ID_RESTAURANT = B.ID_RESTAURANT)
				WHERE A.ID_RESTAURANT =v_id_restaurant;

		END IF;
	END IF;






	
	IF (in_dt_calc <> '0000-00-00' AND in_dt_calc_start = '0000-00-00' AND in_dt_calc_end = '0000-00-00') THEN

		SET v_dt_calc = in_dt_calc;

		CALL calc_ranking_global (v_dt_calc);

	END IF;

	
	IF (in_dt_calc = '0000-00-00' AND in_dt_calc_start <> '0000-00-00' AND in_dt_calc_end <> '0000-00-00') THEN

		SET v_date_done = 0 ;
		SET v_dt_calc = in_dt_calc_start;

		loop_time: LOOP

			IF v_dt_calc > in_dt_calc_end THEN 
				SET v_date_done = 1 ;
			END IF;

			IF v_date_done THEN
				LEAVE loop_time;
			END IF;	

			CALL calc_ranking_global (v_dt_calc);

			SET v_dt_calc = DATE_ADD(v_dt_calc, INTERVAL 1 DAY);

		END LOOP;
	END IF;

	
	IF (in_dt_calc = '0000-00-00' AND in_dt_calc_start = '0000-00-00' AND in_dt_calc_end = '0000-00-00') THEN

		BEGIN
			DECLARE v_dt_score DATE DEFAULT NULL;

			DECLARE v_dates_done INT DEFAULT 0;
			DECLARE cur_dates CURSOR FOR SELECT distinct(DT_SCORE) FROM HIST_SCORE_GLOBAL WHERE NUM_RANKING_GLOBAL = 0;
			DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_dates_done = 1;

			SET v_dates_done = 0;

			OPEN cur_dates;

			dates_loop: LOOP
				FETCH cur_dates INTO v_dt_score;

				IF v_dates_done THEN
					LEAVE dates_loop;
				END IF;

				Call calc_ranking_global (v_dt_score);

			END LOOP;

			CLOSE cur_dates;
		END;

	END IF;

	Call create_log_process ('END', v_num_process_out, 'DB', 'CALC_SCORE_GLOBAL_ALL', NULL, 'OK', v_num_process_in);	
	
END