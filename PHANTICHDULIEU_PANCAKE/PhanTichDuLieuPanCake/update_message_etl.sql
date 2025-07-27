 DECLARE @current_id INT;
            DECLARE @current_message NVARCHAR(MAX);
            DECLARE @key_hashtag NVARCHAR(255);
            DECLARE @key_hashtag_in_dam NVARCHAR(255);

           
            DECLARE message_cursor CURSOR FOR
            SELECT [id], [message], [Key_hashtag]
            FROM [Hallure_datawarehouse].[stg].[hashtag_mapping] where message_etl is null;

            OPEN message_cursor;

            FETCH NEXT FROM message_cursor INTO @current_id, @current_message, @key_hashtag;

            WHILE @@FETCH_STATUS = 0
            BEGIN
                
                SELECT @key_hashtag_in_dam = [Key_hashtag_in_dam]
                FROM [Hallure_datawarehouse].[stg].[Hashtag]
                WHERE [Key_hashtag] = @key_hashtag;

              
                IF @key_hashtag_in_dam IS NOT NULL
                BEGIN
                   
                    SET @current_message = REPLACE(@current_message, @key_hashtag, @key_hashtag_in_dam);

                   
                    UPDATE [Hallure_datawarehouse].[stg].[hashtag_mapping]
                    SET [message_etl] = @current_message
                    WHERE [id] = @current_id;
                END;

                
                FETCH NEXT FROM message_cursor INTO @current_id, @current_message, @key_hashtag;
            END;

            
            CLOSE message_cursor;
            DEALLOCATE message_cursor;