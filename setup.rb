require "google_drive"

# 初回実行時は、表示されるURLにGoogleログイン済のブラウザでアクセスし、表示された文字列を入力する必要あり
GoogleDrive::Session.from_config("google_drive_config.json")
