# SNB GOLD LIVE — M30 Equal Point + M1 Equal Point

بوت إشارات للذهب XAU/USD فقط. البوت لا ينفذ أوامر لدى الوسيط؛ يرسل توصيات ويتابع السعر عبر Telegram.

## المنطق

1. شمعتان مغلقتان M30 بنفس اللون.
2. الفرق بين إغلاق الشمعة الأولى وافتتاح الثانية لا يتجاوز `EQUAL_TOLERANCE` (الافتراضي 0.10$).
3. تُحفظ نقطة التساوي بينهما.
4. عند عودة السعر إلى المستوى، ننتظر شمعتين M1 بنفس اللون وبنقطة تساوي.
5. يدخل البوت فوراً عند إغلاق شمعة M1 الثانية (التي كوّنت نقطة التساوي)، دون انتظار عودة السعر إلى نقطة التساوي.
6. وقف ثابت 4$، والهدف مفتوح.
7. عند ظهور إشارة معاكسة: تُلغى الأوامر المعلقة المعاكسة، وتُوقف متابعة الصفقة النشطة المعاكسة، مع إبقاء سجلها وعدم حذفها.
8. رسالة «اللهم صل...» لا تُرسل إلا بعد نجاح اتصال Telegram وجلب BID/ASK ووجود ثلاث شموع M30 مغلقة على الأقل.

## الملفات

- `snb_gold_live_telegram.py`: البوت.
- `requirements.txt`: الاعتماديات.
- `render.yaml`: إعداد Render.
- `.python-version`: تثبيت Python 3.12 لتفادي مشاكل بناء الحزم على Python 3.14.
- `.env.example`: أسماء المتغيرات فقط، ولا تضع الأسرار في GitHub.

## إعداد GitHub

1. أنشئ Repository خاصاً على GitHub.
2. ارفع الملفات الأربعة، ولا ترفع ملف `.env` أو ملف state.
3. أضف إلى `.gitignore`:

```gitignore
.env
*.session
snb_gold_data/
__pycache__/
*.pyc
```

من PowerShell داخل المجلد:

```powershell
git init
git add .
git commit -m "SNB Gold M30 M1 equal point bot"
git branch -M main
git remote add origin https://github.com/USERNAME/REPOSITORY.git
git push -u origin main
```

## إعداد Render

1. افتح Render ثم **New + → Web Service**.
2. اختر مستودع GitHub.
3. Runtime: Python.
4. Build Command:

```text
pip install -r requirements.txt
```

5. Start Command:

```text
python snb_gold_live_telegram.py
```

لا تحتاج إلى تثبيت `cryptg`؛ هو تسريع اختياري لـ Telethon، وإزالته تمنع فشل البناء على Render. سيعمل Telethon بدونه، مع احتمال بطء بسيط في التشفير.

6. أضف Environment Variables التالية من Render Dashboard، وليس داخل الكود:

- `TELEGRAM_API_ID`
- `TELEGRAM_API_HASH`
- `TELEGRAM_SESSION_STRING`
- `TELEGRAM_CHANNEL`
- `TELEGRAM_ENABLED=true`

الإعدادات الاختيارية موجودة في `.env.example` و`render.yaml`.

## ملاحظات مهمة

- لا تضع `API_HASH` أو `StringSession` في GitHub أو في صورة شاشة.
- `TELEGRAM_CHANNEL` يمكن أن يكون `@channelusername` أو معرفاً مناسباً للحساب.
- الحساب المستخدم في StringSession يجب أن يملك صلاحية الإرسال في القناة.
- Render Free قد يوقف الخدمة أو يعيد تشغيلها؛ لذلك لا تعتمد عليه كضمان لتتبع صفقة أثناء انقطاع الخدمة.
- ملف الحالة يحفظ الصفقات الملغاة والمغلقة ولا يحذف تاريخها. على Render Free، التخزين المحلي ليس دائماً؛ استخدم Disk أو خطة مناسبة إذا أردت حفظ الحالة بعد كل إعادة نشر.
- اختبر أولاً على `TELEGRAM_ENABLED=false` أو قناة خاصة.

## ملاحظات الإصدار المصحّح

- `requirements.txt` يثبّت `dukascopy-python<5`: الإصدار 5 معطوب عند الاستيراد (`NameError: requests`) وبه واجهة مختلفة عن التي يستخدمها الكود.
- المتابعة اللحظية كل `LIVE_POLL_SECONDS` (3 ثوانٍ) بجلب خفيف لآخر 15 دقيقة، وجلب كامل كل دقيقة.
- إشارات أقدم من `SIGNAL_MAX_AGE_MINUTES` تُتجاهل (تمنع إرسال إشارات قديمة بعد كل إعادة تشغيل).
- إيقاف الصفقة المعاكسة وإغلاقها على الوقف يتمان بصمت دون رسائل في القناة.
- الوحدات: الوقف والحركة بالدولار؛ 1$ = 10 نقاط (بيب)، فالوقف 4$ = 40 نقطة.
