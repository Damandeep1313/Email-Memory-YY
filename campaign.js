/*******************************************************
 * campaign.js — Batch Insert Unique Emails with MongoDB (Multi-Campaign)
 *******************************************************/
require("dotenv").config();
const express = require("express");
const mongoose = require("mongoose");
const bodyParser = require("body-parser");
const sgMail = require("@sendgrid/mail");
sgMail.setApiKey(process.env.SENDGRID_API_KEY);

const app = express();
const PORT = process.env.PORT || 3000;
const BASE_MONGO_URI = process.env.MONGO_URI;

if (!BASE_MONGO_URI) {
  throw new Error("❌ MONGO_URI missing in .env");
}

app.use(bodyParser.json());

/*******************************************************
 * CAMPAIGN DATABASE CONFIGURATION
 *******************************************************/
const CAMPAIGNS = {
  campaign1: "campaign1_db",
  campaign2: "campaign2_db", 
  campaign3: "campaign3_db",
  campaign4: "campaign4_db",
  campaign5: "campaign5_db"
};

/*******************************************************
 * MONGOOSE CONNECTION MANAGER
 *******************************************************/
const connections = {};

const getCampaignConnection = (campaignKey) => {
  if (!CAMPAIGNS[campaignKey]) {
    throw new Error(`❌ Invalid campaign: ${campaignKey}`);
  }

  if (!connections[campaignKey]) {
    const dbName = CAMPAIGNS[campaignKey];
    const connectionUri = `${BASE_MONGO_URI}/${dbName}?retryWrites=true&w=majority`;
    
    connections[campaignKey] = mongoose.createConnection(connectionUri, {
      useNewUrlParser: true,
      useUnifiedTopology: true,
    });

    connections[campaignKey].on('error', console.error.bind(console, `❌ MongoDB (${campaignKey}) connection error:`));
    connections[campaignKey].once('open', () => console.log(`✅ MongoDB connected for campaign: ${campaignKey}`));
  }

  return connections[campaignKey];
};

/*******************************************************
 * SCHEMA FACTORY WITH MODEL CACHING
 *******************************************************/
const modelCache = {};

const getContactModel = (campaignKey) => {
  // Return cached model if it exists
  if (modelCache[campaignKey]) {
    return modelCache[campaignKey];
  }

  const connection = getCampaignConnection(campaignKey);
  
  const contactSchema = new mongoose.Schema(
    {
      user_id: String,
      conversation_id: String,
      name: { type: String, default: null },
      email: { type: String, required: true, unique: true },
      company: { type: String, default: null },
      created_at: { type: Date, default: Date.now },

      Title: { type: String, default: "" },
      Firm: { type: String, default: "" },
      Country: { type: String, default: "" },
      "LinkedIn URL": { type: String, default: "" },
    },
    { collection: "contacts" }
  );

  // Cache and return the model
  modelCache[campaignKey] = connection.model('Contact', contactSchema);
  return modelCache[campaignKey];
};

/*******************************************************
 * ENDPOINT: POST /get-unique-emails (Batch Insert)
 *******************************************************/
app.post("/get-unique-emails", async (req, res) => {
  try {
    const { contacts, subject, text } = req.body;
    const user_id = req.header("user_id");
    const conversation_id = req.header("conversation_id");
    const campaign = req.header("campaign"); // REQUIRED - no fallback

    // ✅ STRICT VALIDATION - campaign header is mandatory
    if (!campaign) {
      return res.status(400).json({ error: "Missing campaign header" });
    }

    if (!user_id || !conversation_id) {
      return res.status(400).json({ error: "Missing user_id or conversation_id in headers" });
    }

    if (!CAMPAIGNS[campaign]) {
      return res.status(400).json({ 
        error: `Invalid campaign! Please enter the correct campaign.` 
      });
    }

    if (!Array.isArray(contacts) || contacts.length === 0) {
      return res.status(400).json({ error: "Request must include an array of contacts" });
    }

    const Contact = getContactModel(campaign);
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

    if (newContacts.length === 0) {
      return res.status(200).json({
        message: "These emails already exist in this campaign.",
        campaign: campaign
      });
    }

    const inserted = await Contact.insertMany(newContacts, { ordered: false });
    const responseData = inserted.map(({ name, email, company }) => ({ name, email, company }));
    
    console.log(`✅ Inserted ${inserted.length} contacts into campaign: ${campaign}`);

    await Promise.all(
      responseData.map(async (email) => {
        let sendEmail = email.email.toLowerCase();
        const msg = {
          to: sendEmail,
          from: "Osteopathic Health Centre <wellness@osteopathydubai.com>",
          subject: subject,
          text: text,
          html: `${text}`,
        };
        try {
          await sgMail.send(msg);
          console.log(`✅ Email successfully sent to: ${sendEmail} (Campaign: ${campaign})`);
        } catch (error) {
          console.error(`❌ Failed to send email: ${sendEmail} (Campaign: ${campaign})`);
          console.error(error.toString());
        }
      })
    );

    return res.status(200).json({
      message: `Successfully processed for campaign: ${campaign}`,
      inserted: responseData.length,
      campaign: campaign
    });

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
  console.log(`📊 Available campaigns: ${Object.keys(CAMPAIGNS).join(', ')}`);
});
