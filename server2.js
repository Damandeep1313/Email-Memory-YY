/*******************************************************
 * server.js — Batch Insert Unique Emails with MongoDB
 *******************************************************/
require("dotenv").config();
const express = require("express");
const mongoose = require("mongoose");
const bodyParser = require("body-parser");
const sgMail = require("@sendgrid/mail");
sgMail.setApiKey(process.env.SENDGRID_API_KEY);


const app = express();
const PORT = process.env.PORT || 3000;
const MONGO_URI = process.env.MONGO_URI;

if (!MONGO_URI) {
  throw new Error("❌ MONGO_URI missing in .env");
}

app.use(bodyParser.json());

/*******************************************************
 * MONGOOSE SETUP
 *******************************************************/
mongoose.connect(MONGO_URI, {
  useNewUrlParser: true,
  useUnifiedTopology: true,
});
const db = mongoose.connection;
db.on("error", console.error.bind(console, "❌ MongoDB connection error:"));
db.once("open", () => console.log("✅ MongoDB connected"));

/*******************************************************
 * SCHEMA
 *******************************************************/
const contactSchema = new mongoose.Schema(
  {
    user_id: String,
    conversation_id: String,
    name: { type: String, default: null },
    email: { type: String, required: true, unique: true },
    company: { type: String, default: null },
    created_at: { type: Date, default: Date.now },

    // ✅ New optional fields
    Title: { type: String, default: "" },
    Firm: { type: String, default: "" },
    Country: { type: String, default: "" },
    "LinkedIn URL": { type: String, default: "" },
    
  },
  { collection: "contacts" }
);

const Contact = mongoose.model("Contact", contactSchema);

/*******************************************************
 * ENDPOINT: POST /get-unique-emails (Batch Insert)
 *******************************************************/
app.post("/get-unique-emails", async (req, res) => {
  try {
    const { contacts,subject, text } = req.body;
    const user_id = req.header("user_id");
    const conversation_id = req.header("conversation_id");

    if (!user_id || !conversation_id) {
      return res.status(400).json({ error: "Missing user_id or conversation_id in headers" });
    }

    if (!Array.isArray(contacts) || contacts.length === 0) {
      return res.status(400).json({ error: "Request must include an array of contacts" });
    }

    const incomingEmails = contacts.map(c => c.email);
    const existingDocs = await Contact.find({ email: { $in: incomingEmails } });
    const existingEmails = new Set(existingDocs.map(doc => doc.email));

    const newContacts = contacts
      .filter(c => !existingEmails.has(c.email))
      .map(c => ({
        ...c,
        user_id,
        conversation_id,
      }));

    // If no new contacts were added (all are duplicates)
    if (newContacts.length === 0) {
      return res.status(200).json({
        message: "These emails already exist.",
      });
    }

    const inserted = await Contact.insertMany(newContacts, { ordered: false });

    const responseData = inserted.map(({ name, email, company }) => ({ name, email, company }));
    console.log(responseData);

    await Promise.all(
  responseData.map(async (email) => {
    let sendEmail = email.email.toLowerCase();
    const msg = {
      to: sendEmail,
      from: "on-demand <info@on-demand.io>",
      subject: subject,
      text: text,
      html: `${text}`,
    };
    try {
      await sgMail.send(msg);
      console.log(`✅ Unique address have been contacted.Email successfully sent to: ${sendEmail}`);
    } catch (error) {
      console.error(`❌ Failed to send email: ${sendEmail}`);
      console.error(error.toString());
    }
  })
);

  } catch (err) {
    if (err.code === 11000) {
      return res.status(201).json([]);
    }
    console.error("❌ Error in /get-unique-emails:", err);
    return res.status(500).json({ error: "Internal server error" });
  }
});












/*******************************************************
 * START SERVER
 *******************************************************/
app.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
});
